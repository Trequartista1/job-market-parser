import sys
import os

# -----------------------------
# FIX IMPORT PATH
# -----------------------------
ROOT_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT_DIR not in sys.path:
    sys.path.insert(0, ROOT_DIR)

# -----------------------------
# IMPORTS
# -----------------------------
import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

from cmp.query_parser import parse_query

# -----------------------------
# CONFIG
# -----------------------------
TOKEN = "8563199218:AAHMwOvpNZB21Ki0KhcoefjdhvQrijOqn00"


# -----------------------------
# LOAD DATA
# -----------------------------
def load_data():
    df = pd.read_csv("../../data_2/csvs/final_concatenated1.csv")

    df["search_query"] = df["search_query"].str.lower()
    df["published_datetime"] = pd.to_datetime(df["published_datetime"], errors="coerce")

    return df


df = load_data()


# -----------------------------
# UTILS
# -----------------------------
def parse_skills(skills):
    if isinstance(skills, list):
        return skills

    if isinstance(skills, str):
        return skills.strip("[]").replace("'", "").split(", ")

    return []


def skill_match(row_skills, user_skills):
    row_skills = parse_skills(row_skills)
    return any(skill in row_skills for skill in user_skills)


def format_date(dt):
    if pd.isna(dt):
        return "не указана"
    return dt.strftime("%Y-%m-%d")


# -----------------------------
# FORMAT JOB
# -----------------------------
def format_job(row):

    # salary
    salary = row.get("average_salary")
    if pd.notna(salary) and salary != 0:
        salary = int(salary)
    else:
        salary = "не указано"

    # skills
    skills = parse_skills(row.get("skills"))
    skills_text = ", ".join(skills[:5]) if skills else "не указаны"

    # contact
    contact = row.get("recruiter_phone")
    if pd.isna(contact) or contact == "":
        contact = "не указан"

    # extra fields
    employment = row.get("employment_type") or "не указано"
    experience = row.get("experience_years")
    education = row.get("education") or "не указано"
    source = row.get("source") or "unknown"
    published = format_date(row.get("published_datetime"))

    return (
        f"🧑‍💻 {row['title']}\n"
        f"{row['company']} | {row['location']}\n\n"

        f"💰 Salary: {salary}\n"
        f"📊 Skills: {skills_text}\n"
        f"🕒 Published: {published}\n"

        f"💼 Employment: {employment}\n"
        f"📈 Experience: {experience}\n"
        f"🎓 Education: {education}\n"

        f"📞 Contact: {contact}\n"
        f"🌐 Source: {source}\n\n"

        f"🔗 {row['link']}"
    )


# -----------------------------
# COMMANDS
# -----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "🤖 Job Bot\n\n"
        "Примеры:\n"
        "/jobs python\n"
        "/jobs python sql\n"
    )


async def jobs(update: Update, context: ContextTypes.DEFAULT_TYPE):

    user_input = " ".join(context.args)

    parsed = parse_query(user_input)
    user_skills = parsed.get("skills", [])

    skills_text = ", ".join(user_skills) if user_skills else "не найдено"
    await update.message.reply_text(f"🧠 Skills: {skills_text}")

    filtered_df = df.copy()

    # -----------------------------
    # FILTER
    # -----------------------------
    if user_skills:
        filtered_df = filtered_df[
            filtered_df["skills"].apply(
                lambda x: skill_match(x, user_skills)
            )
        ]

    # -----------------------------
    # EMPTY
    # -----------------------------
    if filtered_df.empty:
        await update.message.reply_text("❌ Ничего не найдено")
        return

    # -----------------------------
    # SORT + LIMIT
    # -----------------------------
    latest = (
        filtered_df
        .sort_values("published_datetime", ascending=False)
        .head(10)   # 🔥 увеличили
    )

    # -----------------------------
    # OUTPUT
    # -----------------------------
    for _, row in latest.iterrows():
        await update.message.reply_text(format_job(row))


# -----------------------------
# MAIN
# -----------------------------
def main():
    print("🚀 Bot started")

    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("jobs", jobs))

    app.run_polling()


if __name__ == "__main__":
    main()