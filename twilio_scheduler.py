import os
import time
from datetime import datetime
import schedule
from twilio.rest import Client
import json
import requests
import pandas as pd
from QualtricsAPI.Setup import Credentials
from QualtricsAPI.XM import XMDirectory
from QualtricsAPI.XM import MailingList
import random
from string import Template
from datetime import timedelta
from dotenv import load_dotenv
load_dotenv()  # read .env into os.environ

# Global variable: 
IMMEDIATE_MODE = True # set to True to send one message immediately to the first contact, or False to run the scheduler at SURVEY_SCHEDULE_TIME daily.
DRY_RUN = True # Global variable: set to True to not actually send messages, but to save them to a CSV instead.
SURVEY_SCHEDULE_TIME = "18:00"  # Set the daily time for scheduling surveys (format HH:MM)

# Load credentials and settings from env vars
ACCOUNT_SID    = os.getenv('TWILIO_ACCOUNT_SID')
AUTH_TOKEN     = os.getenv('TWILIO_AUTH_TOKEN')
TWILIO_NUMBER  = os.getenv('TWILIO_NUMBER')

# Message templates from twilio
TEMPLATES = {
    "DAY_ONE_INVITE": {
        "sid": "HX9f4a98d498eb24f8361b887e3071c929",
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
            contact["StartDate"]      = ed.get("StartDate")

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

def calculate_rewards(completed_count):
    base_reward = completed_count
    bonus = (completed_count // 7) * 7
    return base_reward + bonus

# Function to calculate elapsed days from StartDate (format YYYY-MM-DD) as returned in the Qualtrics fetcher.
def calculate_elapsed_days(start_date_str):
    start_date = datetime.strptime(start_date_str, '%Y-%m-%d').date()
    today = datetime.now().date()
    elapsed_days = (today - start_date).days
    return elapsed_days

def load_contacts(CONTACTS_PATH):
    print(f"Loading contacts from {CONTACTS_PATH}...")
    converters = {
        "phone": lambda x: x.strip('"')
    }
    with open(CONTACTS_PATH, newline='', encoding='utf-8') as f:
        contacts = pd.read_csv(f, converters=converters).to_dict("records")
    return contacts

def send_message(template, phone, content_variables):
    """
    Sends a single message using the specified template, phone, and content variables.
    Assumes that any iterative logic and content preparation (including days_since)
    is handled elsewhere.
    """
    try:
        message = client.messages.create(
            content_sid=template,
            to=f"whatsapp:{phone}",
            from_=f"whatsapp:{TWILIO_NUMBER}",
            content_variables=json.dumps(content_variables)
        )
        print(f"Message sent to {phone}: SID {message.sid}")
        return message
    except Exception as e:
        print(f"Failed to send message to {phone}: {e}")

def get_template_and_variables(contact):
    name = contact.get('firstName', '')
    elapsed = contact.get('elapsed_days')
    # Example logic:
    # For day one, only the name is needed.
    if elapsed == 0:
        template = TEMPLATES["DAY_ONE_INVITE"]["sid"]
        content_variables = {
            "1": name,
            "2": contact.get("extRef", "")
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
            content_variables=content_variables
        )

def schedule_next_survey(contact):
    """
    Determine the next datetime for sending the survey based on SURVEY_SCHEDULE_TIME,
    but only if fewer than 28 days have elapsed since the contact's StartDate.
    Returns the scheduled datetime or None if 28 days have already elapsed.
    """
    # Check if the survey period has expired
    if contact.get("elapsed_days") is None or contact.get("elapsed_days") >= 28:
        print(f"Survey not scheduled for {contact.get("firstName")}: >28 days elapsed.")
        return None

    now = datetime.now()
    hour, minute = map(int, SURVEY_SCHEDULE_TIME.split(":"))
    # Build scheduled datetime for today at the survey time
    scheduled_dt = datetime(now.year, now.month, now.day, hour, minute)
    # If the scheduled time has already passed today, schedule for tomorrow
    if scheduled_dt <= now:
        scheduled_dt += timedelta(days=1)

    return scheduled_dt

if __name__ == "__main__":

    client = Client(ACCOUNT_SID, AUTH_TOKEN)

    contacts = list_all_contacts(
        api_token=os.getenv('QUALTRICS_API_TOKEN'),
        data_center=os.getenv('QUALTRICS_DATA_CENTER'),
        directory_id=os.getenv('QUALTRICS_DIRECTORY_ID'),
        mailing_list_id=os.getenv('QUALTRICS_CONTACT_LIST_ID')
    )
    
    if not contacts:
        print("No contacts found.")
        exit(1)

    # Calculate elapsed days for each contact using their 'StartDate'
    for contact in contacts:
        start_date = contact.get("StartDate")
        contact["CompletedCount"] = int(contact.get("CompletedCount"))
        if start_date:
            contact["elapsed_days"] = calculate_elapsed_days(start_date)
        else:
            contact["elapsed_days"] = None
        contact["next_survey"] = schedule_next_survey(contact)

    if IMMEDIATE_MODE:
        first_contact = next((c for c in contacts if c.get("extRef") == "12345678"), None) 
        template, content_variables = get_template_and_variables(first_contact)
        message = send_message(
            template=template,
            phone=first_contact.get("phone"),
            content_variables=content_variables
        )

    elif not IMMEDIATE_MODE:
        if DRY_RUN:
            print("Dry run mode enabled: Saving scheduled messages to CSV.")
            scheduled_messages = []
            for contact in contacts:
                template_sid, content_variables = get_template_and_variables(contact)
                # Find the corresponding template dictionary by matching the sid.
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
                scheduled_messages.append({
                    "phone": contact['phone'],
                    "name": contact.get("firstName", ""),
                    "template": template_sid,
                    "content_variables": json.dumps(content_variables),
                    "formatted_text": full_text if contact.get("next_survey") else "",
                    "scheduled_at": contact.get("next_survey").strftime("%Y-%m-%d %H:%M:%S") if contact.get("next_survey") else None,
                })
            file_exists = os.path.exists("scheduled_messages.csv")
            df = pd.DataFrame(scheduled_messages)
            df.to_csv("scheduled_messages.csv", mode='a', header=not file_exists, index=False)
            print("Dry run: Scheduled messages appended to scheduled_messages.csv")
        else:
            print(f"Scheduler running; will send at {SURVEY_SCHEDULE_TIME} daily to contacts in {CONTACTS_PATH}.")
            schedule.every().day.at(SURVEY_SCHEDULE_TIME).do(send_all_messages, contacts)
            while True:
                schedule.run_pending()
                time.sleep(30)
