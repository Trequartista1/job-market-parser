import random

# ---------------------------------
# CV LINK
# ---------------------------------

CV_LINK = "https://drive.google.com/file/d/1elovpU51SeUNhz4ZC5lIU1vDLRHs6PvM/view?usp=sharing"

# ---------------------------------
# MESSAGE TEMPLATES
# ---------------------------------

templates = [

    """
Доброго дня.

Зацікавила ваша вакансія {title} у {company}.

Маю pet-project з автоматизації аналітики вакансій:
парсинг work.ua/robota.ua, ETL/data processing, Telegram recommendation bot та automated outreach workflow.

Основний стек:
Python, SQL, pandas, data processing та automation.

Резюме:
{cv_link}
    """,

    """
Вітаю.

Побачив вакансію {title} у {company}.

Реалізував pet-project:
pipeline збору вакансій → обробка даних → Telegram recommendation system → automated outreach workflow.

Працюю з:
Python, SQL, pandas, ETL/data processing.

CV:
{cv_link}
    """,

    """
Доброго дня.

Зацікавила вакансія {title}.

Маю практичний pet-project з автоматизації аналітики ринку вакансій:
- parsing
- ETL/data processing
- Telegram automation
- outreach workflow

Основний стек:
Python, SQL, pandas.

Резюме:
{cv_link}
    """,

    """
Вітаю.

Побачив вашу вакансію {title} у {company}.

Маю досвід роботи з:
Python, SQL, Excel, pandas та data processing.

Також реалізував власний analytics project:
парсинг вакансій → recommendation bot → automated outreach workflow.

CV:
{cv_link}
    """,

    """
Доброго дня.

Зацікавила позиція {title}.

Маю pet-project з аналітики вакансій:
work.ua/robota.ua parsing, ETL/data pipelines, Telegram recommendation system та automation workflow.

Технології:
Python, SQL, pandas, automation.

Резюме:
{cv_link}
    """,

    """
Вітаю.

Побачив вакансію {title}.

Зараз шукаю junior/junior+ позицію в analytics/data напрямку.

Маю pet-project:
job parsing → data processing → Telegram bot → automated outreach workflow.

Стек:
Python, SQL, pandas.

CV:
{cv_link}
    """,

    """
Доброго дня.

Зацікавила вакансія {title} у {company}.

Реалізував власний project з автоматизації аналітики вакансій:
- work.ua/robota.ua parsing
- ETL/data processing
- Telegram recommendation bot
- automated recruiter outreach workflow

Основні інструменти:
Python, SQL, pandas.

Резюме:
{cv_link}
    """
]

# ---------------------------------
# BUILD MESSAGE
# ---------------------------------

def build_message(row):

    template = random.choice(templates)

    message = template.format(
        title=row["title"],
        company=row["company"],
        cv_link=CV_LINK
    )

    return message.strip()