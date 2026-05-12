import pandas as pd
import asyncio
import os
import random

from dotenv import load_dotenv

from telethon import TelegramClient
from telethon.tl.functions.contacts import ImportContactsRequest
from telethon.tl.functions.contacts import DeleteContactsRequest
from telethon.tl.types import InputPhoneContact

# ---------------------------------
# LOAD ENV
# ---------------------------------

load_dotenv()

api_id = int(os.getenv("API_ID"))
api_hash = os.getenv("API_HASH")

# ---------------------------------
# CLIENT
# ---------------------------------

client = TelegramClient(
    "outreach_session",
    api_id,
    api_hash
)

# ---------------------------------
# LOAD DATA
# ---------------------------------

df = pd.read_csv(
    "../../data_2/outreach/outreach_ready.csv"
)

# ---------------------------------
# STORAGE
# ---------------------------------

found_users = []

# ---------------------------------
# MAIN
# ---------------------------------

async def main():

    # login + 2FA
    await client.start(
        phone=lambda: input("PHONE: "),
        password=lambda: input("2FA PASSWORD: ")
    )

    print("\nSUCCESS LOGIN\n")

    # iterate all rows
    for i, row in df.iterrows():

        phone = row["recruiter_phone"]

        # clean number
        phone = (
            phone
            .replace("https://", "")
            .replace("t.me/+", "")
            .replace("t.me/", "")
            .strip()
        )

        print(f"\nChecking: +{phone}")

        try:

            # create temp contact
            contact = InputPhoneContact(
                client_id=i,
                phone="+" + phone,
                first_name="HR",
                last_name=""
            )

            # import contact
            result = await client(
                ImportContactsRequest([contact])
            )

            users = result.users

            # found telegram account
            if users:

                user = users[0]

                print("FOUND")
                print("id:", user.id)

                if user.username:
                    print("username:", user.username)

                # save valid user
                found_users.append({
                    "title": row["title"],
                    "company": row["company"],
                    "recruiter_name": row["recruiter_name"],
                    "recruiter_phone": row["recruiter_phone"],
                    "telegram_id": user.id,
                    "username": user.username,
                    "link": row["link"],
                    "source": row["source"]
                })

                # delete imported contact
                await client(
                    DeleteContactsRequest(
                        id=[user.id]
                    )
                )

            else:

                print("NOT FOUND")

        except Exception as e:

            print("ERROR:", e)

        # random safe delay
        delay = random.randint(45, 90)

        print(f"Sleeping {delay} sec...")

        await asyncio.sleep(delay)

    # ---------------------------------
    # SAVE RESULTS
    # ---------------------------------

    found_df = pd.DataFrame(found_users)

    found_df.to_csv(
        "../../data_2/outreach/valid_telegram_contacts.csv",
        index=False
    )

    print("\nSaved valid_telegram_contacts.csv")

    print(f"\nValid telegram accounts: {len(found_df)}")

    # disconnect
    await client.disconnect()

# ---------------------------------
# RUN
# ---------------------------------

asyncio.run(main())