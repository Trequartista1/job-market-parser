import pandas as pd
from telegram import Update
from telegram.ext import ApplicationBuilder, CommandHandler, ContextTypes

TOKEN = "8563199218:AAHMwOvpNZB21Ki0KhcoefjdhvQrijOqn00"


# -----------------------------
# LOAD DATA
# -----------------------------
def load_data():
    df = pd.read_csv("../../data_2/csvs/final_concatenated1.csv")

    # нормализация
    df["search_query"] = df["search_query"].str.lower()

    return df


df = load_data()


# -----------------------------
# ROLE MAP
# -----------------------------
ROLE_MAP = {
    "data": ["data analyst", "data scientist", "bi analyst"],
    "business": ["business analyst", "system analyst"],
    "marketing": ["marketing analyst"],
    "finance": ["financial analyst"],
    "product": ["product analyst"]
}


# -----------------------------
# FILTERS
# -----------------------------
def filter_by_role(df, role_key):
    role_key = role_key.lower()

    queries = [q.lower() for q in ROLE_MAP.get(role_key, [])]

    return df[df["search_query"].isin(queries)]

def get_avg_salary_by_role(df):
    return df["average_salary"].dropna().mean()

# -----------------------------
# FORMAT
# -----------------------------
def format_job(row, role_avg_salary):

    # зарплата
    if pd.notna(row["average_salary"]):
        salary = int(row["average_salary"])
    else:
        salary = f"~{int(role_avg_salary)} (avg)"

    # контакты
    contact = row.get("recruiter_phone") or "not provided"

    # скиллы
    skills = row.get("skills")

    if isinstance(skills, str):
        skills = skills.strip("[]").replace("'", "").split(", ")

    skills_text = ", ".join(skills[:5]) if skills else "not specified"

    return (
        f"🧑‍💻 {row['title']}\n"
        f"{row['company']} | {row['location']}\n\n"

        f"💰 Salary: {salary}\n"
        f"📊 Skills: {skills_text}\n"
        f"📞 Contact: {contact}\n\n"

        f"🔗 {row['link']}"
    )

# -----------------------------
# COMMANDS
# -----------------------------
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text(
        "📊 Job Market Bot — Data & Analytics Roles\n\n"

        "This bot helps you find relevant jobs based on role and skills.\n"
        "Instead of browsing hundreds of вакансий, you get filtered results.\n\n"

        "🔍 AVAILABLE COMMANDS:\n\n"

        "1. ROLE FILTER (required first argument):\n"
        "/jobs data\n"
        "/jobs business\n"
        "/jobs marketing\n"
        "/jobs finance\n"
        "/jobs product\n\n"

        "2. SKILL FILTER (coming next step):\n"
        "/jobs data python sql\n\n"

        "3. EXAMPLES:\n"
        "/jobs data\n"
        "/jobs business\n\n"

        "📌 HOW IT WORKS:\n"
        "- Jobs are grouped by role\n"
        "- Sorted by latest\n"
        "- (Soon) Ranked by skill match\n\n"

        "💡 TIP:\n"
        "Use 'data' category to access most analytics roles\n"
    )


async def jobs(update: Update, context: ContextTypes.DEFAULT_TYPE):

    filtered_df = df.copy()
    user_args = context.args

    # -----------------------------
    # FILTER 1: ROLE
    # -----------------------------
    if user_args:
        role = user_args[0].lower()

        if role in ROLE_MAP:
            filtered_df = filter_by_role(filtered_df, role)
        else:
            await update.message.reply_text(
                "Unknown category\nExample: /jobs data"
            )
            return

    # -----------------------------
    # EMPTY CHECK
    # -----------------------------
    if filtered_df.empty:
        await update.message.reply_text("No jobs found")
        return

    # 🔥 ВОТ СЮДА ВСТАВЛЯЕМ ↓↓↓

    # средняя зарплата по текущему фильтру
    role_avg_salary = get_avg_salary_by_role(filtered_df)

    # сортировка
    latest = (
        filtered_df
        .sort_values("published_datetime", ascending=False)
        .head(5)
    )

    # вывод
    for _, row in latest.iterrows():
        await update.message.reply_text(
            format_job(row, role_avg_salary)
        )


# -----------------------------
# MAIN
# -----------------------------
def main():
    app = ApplicationBuilder().token(TOKEN).build()

    app.add_handler(CommandHandler("start", start))
    app.add_handler(CommandHandler("jobs", jobs))

    app.run_polling()


if __name__ == "__main__":
    main()