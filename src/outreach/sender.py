import pandas as pd
import asyncio
import random
import os
from datetime import datetime

from dotenv import load_dotenv

from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.errors import PeerFloodError

from message_templates import build_message

# ---------------------------------
# LOAD ENV
# ---------------------------------

load_dotenv()

api_id = int(os.getenv("API_ID"))
api_hash = os.getenv("API_HASH")

# ---------------------------------
# SETTINGS
# ---------------------------------

MAX_MESSAGES_PER_DAY = 20

MIN_DELAY = 300     # 5 min
MAX_DELAY = 1200    # 20 min

# ---------------------------------
# CLIENT
# ---------------------------------

client = TelegramClient(
    "outreach_session",
    api_id,
    api_hash
)

# ---------------------------------
# LOAD CONTACTS
# ---------------------------------

df = pd.read_csv(
    "../../data_2/outreach/valid_telegram_contacts.csv"
)

# ---------------------------------
# LOAD SENT LOG
# ---------------------------------

LOG_PATH = "../../data_2/outreach/sent_log.csv"

if os.path.exists(LOG_PATH):

    sent_log = pd.read_csv(LOG_PATH)

else:

    sent_log = pd.DataFrame(columns=[
        "telegram_id",
        "username",
        "company",
        "date_sent",
        "status"
    ])

# ---------------------------------
# ALREADY SENT IDS
# ---------------------------------

already_sent_ids = set(
    sent_log["telegram_id"].astype(str)
)

# ---------------------------------
# MAIN
# ---------------------------------

async def main():

    await client.start(
        phone=lambda: input("PHONE: "),
        password=lambda: input("2FA PASSWORD: ")
    )

    print("\nSUCCESS LOGIN\n")

    sent_today = 0

    for _, row in df.iterrows():

        # daily limit
        if sent_today >= MAX_MESSAGES_PER_DAY:

            print("\nDAILY LIMIT REACHED")
            break

        telegram_id = str(row["telegram_id"])

        # skip duplicates
        if telegram_id in already_sent_ids:

            print(f"SKIP: already sent -> {telegram_id}")
            continue

        try:

            # build randomized message
            message = build_message(row)

            print("\n---------------------------------")
            print(f"Sending to: {row['company']}")
            print("---------------------------------\n")

            print(message)

            # send message
            await client.send_message(
                entity=int(telegram_id),
                message=message
            )

            print("\nMESSAGE SENT")

            # save log row
            log_row = pd.DataFrame([{
                "telegram_id": telegram_id,
                "username": row.get("username"),
                "company": row.get("company"),
                "date_sent": datetime.now(),
                "status": "sent"
            }])

            sent_log_updated = pd.concat(
                [sent_log, log_row],
                ignore_index=True
            )

            sent_log_updated.to_csv(
                LOG_PATH,
                index=False
            )

            sent_today += 1

            # random safe delay
            delay = random.randint(
                MIN_DELAY,
                MAX_DELAY
            )

            print(f"\nSleeping {delay} sec...\n")

            await asyncio.sleep(delay)

        except FloodWaitError as e:

            print(f"\nFLOOD WAIT: {e.seconds} sec")

            await asyncio.sleep(e.seconds)

        except PeerFloodError:

            print("\nPEER FLOOD DETECTED")
            print("STOPPING SENDER")

            break

        except Exception as e:

            print("\nERROR:", e)

    print("\nSENDER FINISHED")

    await client.disconnect()

# ---------------------------------
# RUN
# ---------------------------------

asyncio.run(main())