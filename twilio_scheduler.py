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
from string import Template
from dotenv import load_dotenv
load_dotenv()  # read .env into os.environ

# Global variable: 
IMMEDIATE_MODE = True # set to True to send one message immediately to the first contact, or False to run the scheduler for all contacts.
DRY_RUN = False # Global variable: set to True to not actually send messages, but to save them to a CSV instead.
SURVEY_SCHEDULE_TIME = "12:00"  # Set the daily time for scheduling surveys (format HH:MM)
SCHEDULE_MODE       = "daily"        # "daily" or "interval"
INTERVAL_MINUTES    = 10              # used only when SCHEDULE_MODE == "interval"


# Load credentials and settings from env vars
ACCOUNT_SID    = os.getenv('TWILIO_ACCOUNT_SID')
AUTH_TOKEN     = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_NUMBER  = os.getenv('TWILIO_NUMBER')
CONVERSATIONS_SERVICE_SID = os.getenv("TWILIO_CONVERSATIONS_SERVICE_SID")

# Message templates from twilio
TEMPLATES = {
    "DAY_ONE_INVITE": {
        "sid": "HX04f99dd2b6a51504edcf176741dbaf0f",
        "variables": ["name", "survey_schedule_time", "RANDOM_ID"],
        "text": "Hi $name! 👋 Welcome to the METL lab daily media use study – we're excited to have you on board! Starting today, you'll receive a short message daily at $survey_schedule_time with a link to your survey. It only takes 3–4 minutes to complete each day. Please try to fill it out whenever suits your routine best. You can contact us with any questions by simply replying to us in this chat."
    },
    "BASIC_INVITE": {
        "sid": "HX45b9d150bb0e3791e5b477d8dc3db2aa",
        "variables": ["name", "days_since_enrollment", "RANDOM_ID"],
        "text": "Hi $name, daily survey #$days_since_enrollment is now ready to be completed using the link below!"
    },
    "REWARD_INVITE": {
        "sid": "HX2751307dc93f95f5580e26661053e462",
        "variables": ["name", "rewards_to_date", "time_until_next_reward", "RANDOM_ID"],
        "text": "Hi $name, your daily survey is now available! You've earned £$rewards_to_date so far, and are just $time_until_next_reward days away from receiving your next £7 bonus. Thanks for your ongoing participation."
    }
}

# Create Template objects for later formatting.
for key, value in TEMPLATES.items():
    value["template_obj"] = Template(value["text"])

# Sanity check
if not all([ACCOUNT_SID, AUTH_TOKEN, TWILIO_NUMBER]):
    raise RuntimeError(
        "Please set TWILIO_ACCOUNT_SID, TWILIO_AUTH_TOKEN and TWILIO_NUMBER in your .env"
    )

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

def ensure_identity_participant(conv_sid, identity):
    """
    Make sure a Chat‐SDK identity (e.g. "user01") is on the conversation.
    """
    svc = client.conversations.v1.services(CONVERSATIONS_SERVICE_SID)
    print(f"[ensure_identity] Checking for identity '{identity}' in convo {conv_sid}")
    participants = svc.conversations(conv_sid).participants.list()
    existing = {p.identity for p in participants if p.identity}
    if identity in existing:
        print(f"[ensure_identity] Identity '{identity}' already present")
        return
    try:
        svc.conversations(conv_sid).participants.create(identity=identity)
        print(f"[ensure_identity] Added identity participant '{identity}'")
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
        print(f"[get_or_create] Fetching convo with unique_name='{contact_phone}'")
        conv = svc.conversations(contact_phone).fetch()
        print(f"[get_or_create] Found convo SID={conv.sid}")
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
        print(f"[get_or_create] Adding WA participant address={user_uri}")
        svc.conversations(conv.sid).participants.create(
            messaging_binding_address=user_uri,
            messaging_binding_proxy_address=proxy_uri
        )
        print(f"[get_or_create] WA participant added")
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

# Function to calculate elapsed days from EnrollmentDate (format YYYY-MM-DD) as returned in the Qualtrics fetcher.
def calculate_elapsed_days(start_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    today = datetime.now().date()
    elapsed_days = (today - start_date).days + 1 # +1 to include today
    return elapsed_days

def load_contacts(CONTACTS_PATH):
    print(f"Loading contacts from {CONTACTS_PATH}...")
    converters = {
        "phone": lambda x: x.strip('"')
    }
    with open(CONTACTS_PATH, newline='', encoding='utf-8') as f:
        contacts = pd.read_csv(f, converters=converters).to_dict("records")
    return contacts

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

def send_all_messages(contacts):
    for contact in contacts:
        # Only send a message if next_survey is set (valid scheduling)
        if not contact.get("next_survey"):
            print("Skipping contact; study window is completed:", contact.get("extRef"))
            continue

        phone = contact.get('phone')
        if not phone:
            print("Skipping contact with missing phone:", contact)
            continue

        template, content_variables = get_template_and_variables(contact)
        send_message(
            template=template,
            phone=phone,
            content_variables=content_variables,
            extRef=contact.get("extRef")
        )

def schedule_next_survey(contact):
    """
    Determine the next datetime for sending the survey based on SURVEY_SCHEDULE_TIME,
    but only if fewer than 28 days have elapsed since the contact's EnrollmentDate.
    Returns the scheduled datetime or None if 28 days have already elapsed.
    """
    # Check if the survey period has expired
    if contact["elapsed_days"] >= 28:
        print(f"Survey not scheduled for {contact.get('firstName')}: ≥28 days elapsed.")
        return None

    now = datetime.now()
    hour, minute = map(int, SURVEY_SCHEDULE_TIME.split(":"))
    scheduled_dt = datetime(now.year, now.month, now.day, hour, minute)
    if scheduled_dt <= now:
        scheduled_dt += timedelta(days=1)
        
    return scheduled_dt

def pull_contacts(IMMEDIATE_MODE=False): 
    
    # Pull contacts fresh before each scheduled run.
    contacts = list_all_contacts(
        api_token=os.getenv('QUALTRICS_API_TOKEN'),
        data_center=os.getenv('QUALTRICS_DATA_CENTER'),
        directory_id=os.getenv('QUALTRICS_DIRECTORY_ID'),
        mailing_list_id=os.getenv('QUALTRICS_CONTACT_LIST_ID')
    )
    if not contacts:
        print("No contacts found.")
        return

    # Calculate elapsed days and schedule next survey for each contact.
    for contact in contacts:
        # 1) Robustly coerce CompletedCount → int, defaulting to 0
        try:
            contact["CompletedCount"] = int(contact.get("CompletedCount") or 0)
        except (TypeError, ValueError):
            contact["CompletedCount"] = 0

        # 2) Compute elapsed_days: if we have a date, calc it; else treat as day 0
        start_date = contact.get("EnrollmentDate")
        if start_date:
            contact["elapsed_days"] = calculate_elapsed_days(start_date)
        else:
            contact["elapsed_days"] = 1

        # 3) Always try to schedule—schedule_next_survey will only bail after 28 days
        contact["next_survey"] = schedule_next_survey(contact)

    if IMMEDIATE_MODE:
        # If in immediate mode, send a message to the first contact.
        if contacts:
            immediate_contact = next((c for c in contacts if c.get("extRef") == "12345678"), None)
            immediate_contact["next_survey"] = datetime.now()
            print("Immediate mode: sending message to first contact.")
            contacts = [immediate_contact]
            
    print("Contacts pulled and processed: ", contacts)
    return(contacts)

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

    contacts = pull_contacts(IMMEDIATE_MODE=IMMEDIATE_MODE)
    if not contacts:
        print("No contacts found.")
        return

    now_iso = datetime.now().isoformat()
    rows = []
    for contact in contacts:
        conv_sid = get_or_create_conversation_for(contact["phone"])
        sid, vars_ = get_template_and_variables(contact)
        add_message_text_to_contact(contact, sid, vars_)
        rows.append({
            "run_time": now_iso,
            "phone": contact["phone"],
            "template_sid": sid,
            "content_variables": json.dumps(vars_),
            "formatted_text": contact["formatted_text"],
            "scheduled_time": contact["next_survey"].isoformat() if contact["next_survey"] else None,
            "dry_run": DRY_RUN,
        })

        if not DRY_RUN:
            send_message(
                template_sid=sid,
                conversation_sid=conv_sid,
                content_variables=vars_,
                extRef=contact.get("extRef", ""),
                full_text = contact["formatted_text"],  
            )

    # append to CSV log
    df = pd.DataFrame(rows)
    file_exists = os.path.exists("scheduled_messages.csv")
    df.to_csv("scheduled_messages.csv", mode='a', header=not file_exists, index=False)
    print(f"{len(rows)} messages logged{' (not sent)' if DRY_RUN else ''}.")

if __name__ == "__main__":

    client = Client(ACCOUNT_SID, AUTH_TOKEN)

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