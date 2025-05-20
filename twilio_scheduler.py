import os
import sys
import time
from datetime import datetime, timedelta, timezone
import schedule
from twilio.rest import Client
from twilio.base.exceptions import TwilioRestException
import json
import requests
import pandas as pd
import random
import re
from string import Template
from dotenv import load_dotenv
import phonenumbers
load_dotenv()  # read .env into os.environ

# ─── CONFIGURATION ────────────────────────────────────────────────────
IMMEDIATE_MODE        = False   # send only first contact if True
DRY_RUN               = False    # if True, log but don’t actually send
SURVEY_SCHEDULE_TIME  = "12:00" # HH:MM daily send time
SCHEDULE_MODE         = "daily" # "daily" or "interval"
INTERVAL_MINUTES      = 10      # if using interval mode
# ───────────────────────────────────────────────────────────────────────

# Load credentials and settings from env vars
ACCOUNT_SID    = os.getenv('TWILIO_ACCOUNT_SID')
AUTH_TOKEN     = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_NUMBER  = os.getenv('TWILIO_NUMBER')
CONVERSATIONS_SERVICE_SID = os.getenv("TWILIO_CONVERSATIONS_SERVICE_SID")
client = None

if not all([ACCOUNT_SID, AUTH_TOKEN, TWILIO_NUMBER, CONVERSATIONS_SERVICE_SID]):
    missing = [n for n, v in [
        ("TWILIO_ACCOUNT_SID", ACCOUNT_SID),
        ("TWILIO_AUTH_TOKEN", AUTH_TOKEN),
        ("TWILIO_NUMBER", TWILIO_NUMBER),
        ("TWILIO_CONVERSATIONS_SERVICE_SID", CONVERSATIONS_SERVICE_SID)
    ] if not v]
    raise RuntimeError(f"Please set {', '.join(missing)} in your .env")

# Message templates from twilio
TEMPLATES = {
    "DAY_ONE_INVITE": {
        "sid": "HX6e5455d20963fa9c4ae177bb7f6786f7",
        "variables": ["name", "survey_schedule_time", "RANDOM_ID"],
        "text": "Hi $name! 👋 Welcome to the METL lab daily media use study – we're excited to have you on board! Starting today, you'll receive a short message daily at $survey_schedule_time with a link to your survey. It only takes 3–4 minutes to complete each day. Please try to fill it out whenever suits your routine best. You can contact us with any questions by simply replying to us in this chat."
    },
    "BASIC_INVITE": {
        "sid": "HX3a609e60d56b27858bf573111b9d6827",
        "variables": ["name", "days_since_enrollment", "RANDOM_ID"],
        "text": "Hi $name, daily survey #$days_since_enrollment is now ready to be completed using the link below!"
    },
    "REWARD_INVITE": {
        "sid": "HX5aa71f2c62e59e67d89f560a0537a141",
        "variables": ["name", "rewards_to_date", "time_until_next_reward", "RANDOM_ID"],
        "text": "Hi $name, your daily survey is now available! You've earned £$rewards_to_date so far, and are just $time_until_next_reward days away from receiving your next £7 bonus. Thanks for your ongoing participation."
    }
}

# Create Template objects for later formatting.
for key, value in TEMPLATES.items():
    value["template_obj"] = Template(value["text"])
    
# Set up logging
_logfile = open('scheduler.log', 'a')

class _Tee:
    def __init__(self, *files):
        self.files = files
    def write(self, data):
        for f in self.files:
            f.write(data)
    def flush(self):
        for f in self.files:
            f.flush()

# replace stdout and stderr with our Tee
sys.stdout = _Tee(sys.stdout, _logfile)
sys.stderr = _Tee(sys.stderr, _logfile)

#### Functions ####

def list_all_contacts(api_token, data_center, directory_id, mailing_list_id, page_size=1000):
    """
    Retrieve all contacts in a Qualtrics mailing list via the REST API,
    including any embeddedData fields.

    Args:
      api_token (str): Your Qualtrics API token.
      data_center (str): Your data center ID (e.g. 'fra1').
      directory_id (str): The directory (pool) ID, e.g. 'POOL_XXXXXXXXX'.
      mailing_list_id (str): The mailing-list ID, e.g. 'CG_XXXXXXXXX'.
      page_size (int): How many records to fetch per call (max 1000).

    Returns:
      list[dict]: A list of contact objects, each with an 'embeddedData' dict.
    """
    base_url = (
        f"https://{data_center}.qualtrics.com/API/v3/"
        f"directories/{directory_id}/mailinglists/{mailing_list_id}/contacts"
    )
    headers = {
        "X-API-TOKEN": api_token,
        "Content-Type": "application/json"
    }
    # includeEmbedded must be true to get embeddedData back
    params = {
        "pageSize": page_size,
        "includeEmbedded": "true"
    }

    all_contacts = []
    while True:
        resp = requests.get(base_url, headers=headers, params=params)
        resp.raise_for_status()
        body = resp.json()

        # sanity check
        if "result" not in body or "elements" not in body["result"]:
            raise RuntimeError(f"Unexpected API response: {body}")

        for contact in body["result"]["elements"]:
            # now embeddedData will actually be a dict, not empty
            ed = contact.get("embeddedData", {})
            # pull out your specific fields (or just keep ed if you want them all)
            contact["CompletedCount"] = ed.get("CompletedCount")
            contact["EnrollmentDate"]      = ed.get("EnrollmentDate")

        all_contacts.extend(body["result"]["elements"])

        next_page = body["result"].get("nextPage")
        if not next_page:
            break

        # handle either full-URL or skipToken
        if next_page.startswith("http"):
            base_url = next_page
            params = {}
        else:
            params = {
                "pageSize": page_size,
                "includeEmbedded": "true",
                "skipToken": next_page
            }

    return all_contacts

def normalize_phone_number(raw_phone, default_region='GB'):
    """
    Take any human‐entered phone string and return a valid E.164 string,
    or None if it can't be parsed/validated.
    """
    # Strip out everything except digits and '+'
    cleaned = ''.join(ch for ch in raw_phone if ch.isdigit() or ch == '+')

    try:
        # If the user included a country code (“+”), parse without a region hint
        if cleaned.startswith('+'):
            num = phonenumbers.parse(cleaned, None)
        else:
            # No country code, assume default_region
            num = phonenumbers.parse(cleaned, default_region)

        # Only accept if it's a valid number
        if not phonenumbers.is_valid_number(num):
            return None

        # Return in E.164 (“+447352000107”)
        return phonenumbers.format_number(num, phonenumbers.PhoneNumberFormat.E164)

    except phonenumbers.NumberParseException:
        return None

def ensure_identity_participant(conv_sid, identity):
    """
    Make sure a Chat‐SDK identity (e.g. "user01") is on the conversation.
    """
    svc = client.conversations.v1.services(CONVERSATIONS_SERVICE_SID)
    participants = svc.conversations(conv_sid).participants.list()
    existing = {p.identity for p in participants if p.identity}
    if identity in existing:
        print(f"[ensure_identity] Identity '{identity}' present in convo {conv_sid}")
        return
    try:
        svc.conversations(conv_sid).participants.create(identity=identity)
    except TwilioRestException as e:
        print(f"[ensure_identity] ERROR adding identity '{identity}' → {e.status}: {e.msg}")

def get_or_create_conversation_for(contact_phone):
    """
    Fetch by unique_name or create new convo, then
    add WA binding + ensure identity is present.
    """
    user_uri  = f"whatsapp:{contact_phone}"
    proxy_uri = f"whatsapp:{TWILIO_NUMBER}"
    svc       = client.conversations.v1.services(CONVERSATIONS_SERVICE_SID)

    # 1) Try fetch
    try:
        print(f"[get_or_create] Processing convo for '{contact_phone}'")
        conv = svc.conversations(contact_phone).fetch()
    except TwilioRestException as e:
        if e.status != 404:
            print(f"[get_or_create] ERROR fetching convo → {e.status}: {e.msg}")
            raise
        # 404 → create new
        print(f"[get_or_create] Not found, creating new convo for '{contact_phone}'")
        conv = svc.conversations.create(
            unique_name=contact_phone,
            friendly_name=f"{contact_phone}"
        )
        print(f"[get_or_create] Created convo SID={conv.sid}")

    # 2) Add WhatsApp participant (ignore if already exists)
    try:
        svc.conversations(conv.sid).participants.create(
            messaging_binding_address=user_uri,
            messaging_binding_proxy_address=proxy_uri
        )
    except TwilioRestException as e:
        print(f"[get_or_create] WA binding exists or failed → {e.status}: {e.msg}")
    time.sleep(.1)
    # 3) Ensure your identity is on the convo
    ensure_identity_participant(conv.sid, "user01")

    return conv.sid

def is_session_open(
    conversation_sid: str,
    bot_identity: str = "user01",
    lookback: int = 10
) -> bool:
    """
    Returns True if, among the last `lookback` messages in the conversation,
    the most recent one from someone other than `bot_identity` was sent
    less than 24 hours ago.
    """
    svc = client.conversations.v1.services(CONVERSATIONS_SERVICE_SID)
    convo = svc.conversations(conversation_sid)

    try:
        recent = convo.messages.list(page_size=lookback, order="desc")
    except TwilioRestException as e:
        print(f"[is_session_open] ERROR fetching messages: {e.status} {e.msg}")
        return False

    # Find the first message not sent by the bot
    for msg in recent:
        if msg.author != bot_identity:
            # Compare aware datetimes
            now_utc = datetime.now(timezone.utc)
            age = now_utc - msg.date_created
            print(
                f"[is_session_open] Last user msg at {msg.date_created.isoformat()}, "
                f"age={age.total_seconds()/3600:.1f}h"
            )
            return age < timedelta(hours=24)

    # No user message found in the recent window
    print("[is_session_open] No recent user message found")
    return False

def calculate_rewards(completed_count):
    base_reward = completed_count
    bonus = (completed_count // 7) * 7
    return base_reward + bonus

def calculate_elapsed_days(start_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    today = datetime.now().date()
    elapsed_days = (today - start_date).days 
    return elapsed_days

def send_message(template_sid, conversation_sid, content_variables, extRef, full_text):
    """
    Sends either a session (body) message if the 24h window is open,
    or a template (content_sid) otherwise. Flags attrs.system,
    includes surveyUrl and participantId in attributes.
    """
    print(f"[send_message] → Convo SID: {conversation_sid}")
    print(f"               Template SID: {template_sid}")
    print(f"               Content Vars: {content_variables}")

    survey_url = (
        f"https://oii.qualtrics.com/jfe/form/SV_9tSCJIEm6mf2ezA"
        f"?RANDOM_ID={extRef}"
    )
    attrs = {
        "system": True,
        "surveyUrl": survey_url,
        "participantId": extRef
    }

    svc  = client.conversations.v1.services(CONVERSATIONS_SERVICE_SID)
    convo = svc.conversations(conversation_sid)

    use_session = is_session_open(conversation_sid)
    print(f"[send_message] Using {'SESSION' if use_session else 'TEMPLATE'} message")

    try:
        if use_session:
            msg = convo.messages.create(
                author="user01",
                body = f"{full_text}\n\n{survey_url}",       # <— use the precomputed text
                attributes=json.dumps(attrs)
            )
            print(f"[send_message] ✓ Sent SESSION message SID={msg.sid}")
        else:
            msg = convo.messages.create(
                author="user01",
                content_sid=template_sid,
                content_variables=json.dumps(content_variables),
                attributes=json.dumps(attrs)
            )
            print(f"[send_message] ✓ Sent TEMPLATE message SID={msg.sid}")

        return msg

    except TwilioRestException as e:
        print(f"[send_message] ✗ Error sending message → {e.status}: {e.msg}")
        raise

def get_template_and_variables(contact):
    name = contact.get('firstName')
    elapsed = contact.get('elapsed_days')
    
    # For day one, we need name, survey schedule time, and a ID.
    if elapsed == 1:
        template = TEMPLATES["DAY_ONE_INVITE"]["sid"]
        content_variables = {
            "1": name,
            "2": SURVEY_SCHEDULE_TIME,
            "3": contact.get("extRef", "")
        }
    # For early days, use basic invite: name and days since enrollment.
    else:
        invitation_type = random.choice(["basic", "reward"])
        if invitation_type == "basic":
            template = TEMPLATES["BASIC_INVITE"]["sid"]
            content_variables = {
                "1": name,
                "2": str(contact.get("elapsed_days", 0)),
                "3": contact.get("extRef", "")
            }
        else:
            completed_count = contact.get("CompletedCount", 0)
            rewards = calculate_rewards(completed_count)
            next_multiple = ((completed_count // 7) + 1) * 7
            time_until_next = str(next_multiple - completed_count)
            template = TEMPLATES["REWARD_INVITE"]["sid"]
            content_variables = {
                "1": name,
                "2": str(rewards),
                "3": time_until_next,
                "4": contact.get("extRef", "")
            }
    return template, content_variables

def schedule_next_survey(contact):
    """
    Schedules the next survey for a contact based on their elapsed days.
    """
    now = datetime.now()
    hour, minute = map(int, SURVEY_SCHEDULE_TIME.split(":"))
    scheduled_dt = datetime(now.year, now.month, now.day, hour, minute)
    if scheduled_dt <= now:
        scheduled_dt += timedelta(days=1)
        
    return scheduled_dt

def pull_contacts(IMMEDIATE_MODE=False): 
    """
    Pull contacts from Qualtrics, compute elapsed days (day 1 = EnrollmentDate + 1),
    and schedule the next survey for days 1–27 only.
    """
    # 1) Fetch all contacts (with embedded EnrollmentDate & CompletedCount)
    contacts = list_all_contacts(
        api_token=os.getenv('QUALTRICS_API_TOKEN'),
        data_center=os.getenv('QUALTRICS_DATA_CENTER'),
        directory_id=os.getenv('QUALTRICS_DIRECTORY_ID'),
        mailing_list_id=os.getenv('QUALTRICS_CONTACT_LIST_ID')
    )
    if not contacts:
        print("No contacts found.")
        return

    # 2) Compute elapsed_days & next_survey for each
    for contact in contacts:
        # Robustly coerce CompletedCount → int
        try:
            contact["CompletedCount"] = int(contact.get("CompletedCount") or 0)
        except (TypeError, ValueError):
            contact["CompletedCount"] = 0

        # Must have an EnrollmentDate in YYYY-MM-DD
        start_date = contact.get("EnrollmentDate")
        if not start_date:
            print(f"Skipping extRef={contact.get('extRef')} due to missing EnrollmentDate")
            contact["elapsed_days"] = None
            contact["next_survey"] = None
            continue

        # Calculate how many days since enrollment; if today = enrollment + 1 → elapsed_days == 1
        contact["elapsed_days"] = calculate_elapsed_days(start_date)

        # Only schedule surveys for days 1 through 27
        if 1 <= contact["elapsed_days"] < 28:
            contact["next_survey"] = schedule_next_survey(contact)
        else:
            contact["next_survey"] = None

    # 3) (Optional) Immediate-mode override for testing
    if IMMEDIATE_MODE and contacts:
        immediate = next((c for c in contacts if c.get("extRef") == "12345678"), None)
        if immediate:
            immediate["next_survey"] = datetime.now()
            print("Immediate mode: forcing survey for", immediate.get("extRef"))
            contacts = [immediate]

    print("Contacts pulled and processed:", contacts)
    return contacts

def add_message_text_to_contact(contact, template_sid, content_variables):
    """
    Computes the formatted message text for a contact and adds it to the contact dict
    under the key 'formatted_text'.
    """
    # Find the corresponding template by matching the sid.
    tmpl_dict = None
    for tmpl in TEMPLATES.values():
        if tmpl["sid"] == template_sid:
            tmpl_dict = tmpl
            break
    # Prepare the formatted text using the template's variable names.
    if tmpl_dict:
        mapping = {}
        for idx, var_name in enumerate(tmpl_dict["variables"], start=1):
            mapping[var_name] = content_variables.get(str(idx), "")
        full_text = tmpl_dict["template_obj"].safe_substitute(mapping)
    else:
        full_text = ""
    contact["formatted_text"] = full_text if contact.get("next_survey") else ""
    return contact

def run_job():
    """
    Pulls contacts, computes next-survey times, builds messages,
    sends them (unless DRY_RUN), and logs everything to CSV.
    """
    global client
    if client is None:
        client = Client(ACCOUNT_SID, AUTH_TOKEN)
        print(f"→ Twilio Client initialized (Account SID {ACCOUNT_SID})")

    contacts = pull_contacts(IMMEDIATE_MODE=IMMEDIATE_MODE)
    if not contacts:
        print("No contacts found.")
        return

    now_iso = datetime.now().isoformat()
    rows = []

    for contact in contacts:
        # 1) Skip if no schedule
        if not contact.get("next_survey"):
            print(f"Skipping extRef={contact.get('extRef')} (day {contact.get('elapsed_days')})")
            continue

        # 2) Validate phone
        phone_raw = (contact.get("phone") or "").strip()
        normalized = normalize_phone_number(phone_raw)
        if not normalized:
            print(f"Skipping invalid phone '{phone_raw}' for extRef={contact.get('extRef')}")
            continue

        phone = normalized  # replace with the cleaned, validated version

        # 3) Build/fetch convo & message vars
        conv_sid = get_or_create_conversation_for(phone)
        # ——— OVERRIDE FOR US DAY-1 UTILITY INVITE ———
        # If it's day 1, the number is US (+1…), and there is no open session,
        # send the BASIC_INVITE (utility) instead of the Day 1 marketing template.
        if contact.get("elapsed_days") == 1 \
           and phone.startswith("+1") \
           and not is_session_open(conv_sid):
            template_sid = TEMPLATES["BASIC_INVITE"]["sid"]
            vars_ = {
                "1": contact.get("firstName",""),
                "2": str(contact.get("elapsed_days", 1)),
                "3": contact.get("extRef","")
            }
        else:
            template_sid, vars_ = get_template_and_variables(contact)

        # 4) Attempt send
        status = "QUEUED"
        error  = ""
        if not DRY_RUN:
            try:
                send_message(
                    template_sid=template_sid,
                    conversation_sid=conv_sid,
                    content_variables=vars_,
                    extRef=contact.get("extRef", ""),
                    full_text=contact["formatted_text"],
                )
            except TwilioRestException as e:
                status = "FAILED"
                error  = f"{e.status}: {e.msg}"
                print(f"[run_job] ❌ Failed to send to {phone} → {error}")

        # 5) Log the attempt
        rows.append({
            "run_time":        now_iso,
            "phone":           phone,
            "template_sid":    template_sid,
            "content_variables": json.dumps(vars_),
            "formatted_text":  contact["formatted_text"],
            "scheduled_time":  contact["next_survey"].isoformat(),
            "dry_run":         DRY_RUN,
            "status":          status,
            "error":           error,
        })

    # 6) Write CSV
    if rows:
        df = pd.DataFrame(rows)
        file_exists = os.path.exists("scheduled_messages.csv")
        df.to_csv(
            "scheduled_messages.csv",
            mode='a',
            header=not file_exists,
            index=False
        )
        print(f"{len(rows)} message(s) logged{' (not sent)' if DRY_RUN else ''}.")
    else:
        print("No messages to log.")

if __name__ == "__main__":

    # Immediate mode send
    if IMMEDIATE_MODE:
        run_job()

        # choose your scheduling strategy
    if SCHEDULE_MODE == "interval":
        schedule.every(INTERVAL_MINUTES).minutes.do(run_job)
        print(f"{datetime.now().isoformat()} - Scheduler running; will send survey every {INTERVAL_MINUTES} minutes.")
    else:
        schedule.every().day.at(SURVEY_SCHEDULE_TIME).do(run_job)
        print(f"{datetime.now().isoformat()} - Scheduler running; will send survey at {SURVEY_SCHEDULE_TIME} daily.")

    # Loop
    while True:
        schedule.run_pending()
        time.sleep(60)  # check every 2 minutes