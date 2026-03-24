import pandas as pd
import numpy as np
import ast
from datetime import datetime, timedelta
import re

# считывание
workua = pd.read_csv("../../data_2/raw/workua_set.csv")
rabotaua = pd.read_csv("../../data_2/raw/robotaua_set.csv")

# генерация ссылок на тг вместо номера
mask = (workua["company_contact"].str.contains(r"\d", na=False) &   # есть цифры
       ~workua["company_contact"].str.contains(":", na=False)      # нет ссылок (http, tel)
       )

workua.loc[mask, "recruiter_phone"] = workua.loc[mask, "company_contact"] # переносим контакты

workua["recruiter_phone"] = (workua["recruiter_phone"].fillna("").str.replace(r"\D", "", regex=True)) # оставляем только цифры

def normalize_phone(x): # функция нормализации номеров
    if not x:
        return None

    # UA номера
    if x.startswith("380") and len(x) == 12:
        return "t.me/+" + x

    if x.startswith("0") and len(x) == 10:
        return "t.me/+38" + x

    # международные (пример: 34..., 48..., etc.)
    if len(x) >= 10 and len(x) <= 13:
        return "t.me/+" + x

    return None  # мусор

workua["recruiter_phone"] = workua["recruiter_phone"].apply(normalize_phone)

workua = workua.drop(columns="company_contact")

rabotaua["recruiter_phone"] = (rabotaua["recruiter_phone"].fillna("").str.replace(r"\D", "", regex=True)) # оставляем только цифры

rabotaua["recruiter_phone"] = rabotaua["recruiter_phone"].apply(normalize_phone) # делаем тг ссылку из номера

# нормализация дат
def parse_relative_date(text): # функция нормализации даты публикации для работы
    if not text or not isinstance(text, str):
        return None

    text = text.lower()
    now = datetime.now()

    num = re.search(r"\d+", text)
    value = int(num.group()) if num else 0

    if "мин" in text:
        dt = now - timedelta(minutes=value)

    elif "час" in text:
        dt = now - timedelta(hours=value)

    elif "дн" in text:
        dt = now - timedelta(days=value)

    elif "нед" in text:
        dt = now - timedelta(weeks=value)

    elif "мес" in text:
        dt = now - timedelta(days=30 * value)

    else:
        return None

    return dt.replace(microsecond=0)

rabotaua["published_datetime"] = rabotaua["published_datetime"].apply(parse_relative_date)

# конкат 2 фреймов
temp = pd.concat([workua, rabotaua])

#средняя зп
temp.insert(temp.columns.get_loc("salary_max") + 1, "average_salary", (temp["salary_max"] + temp["salary_min"]) / 2)

#конвертация даты
temp['published_datetime'] = pd.to_datetime(temp['published_datetime'])

#отсеивание без скиллов
filtered = temp[temp["skills"] != "[]"]

#работа с пропусками в зп, для среднего
filtered["salary_min"] = filtered["salary_min"].replace(0, np.nan)
filtered["salary_max"] = filtered["salary_max"].replace(0, np.nan)

#дополнение сорс, для фильтрации
filtered.loc[filtered["source"].isna(), "source"] = 'workua'

#заполнение пропусков дат текущим моментом
filtered.loc[filtered['published_datetime'].isna(), "published_datetime"] = pd.Timestamp.now()

#средние в разрезе типа должности
averages = filtered.groupby('search_query')['average_salary'].mean()
filtered["average_salary"] = filtered["average_salary"].fillna(filtered["search_query"].map(averages))

#filtered["average_salary"] = filtered["average_salary"].astype("Int64")

#нормализация скилов
SKILL_MAP = {
    "python": ["python"],
    "sql": ["sql", "postgres", "mysql", "pl/sql"],
    "excel": ["excel", "google sheets", "sheets"],
    "power bi": ["power bi", "powerbi"],
    "tableau": ["tableau"],
    "pandas": ["pandas"],
    "numpy": ["numpy"],
    "statistics": ["statistics"],
    "machine learning": ["machine learning", "ml"],
    "etl": ["etl"],
    "api": ["api"],
    "git": ["git", "github", "gitlab"],
    "aws": ["aws"],
    "gcp": ["gcp"],
    "azure": ["azure"]
}

def process_skills(x):
    # 1. строка → список
    if isinstance(x, str):
        try:
            skills_list = ast.literal_eval(x)
        except:
            return []
    elif isinstance(x, list):
        skills_list = x
    else:
        return []

    # 2. нормализация + фильтрация
    result = set()

    for skill in skills_list:
        if not isinstance(skill, str):
            continue

        s = skill.lower().strip()

        for normalized, variants in SKILL_MAP.items():
            if any(v in s for v in variants):
                result.add(normalized)
                break

    return list(result)

# применяем
filtered["skills"] = filtered["skills"].apply(process_skills)

# удаляем пустые
filtered = filtered[filtered["skills"].apply(len) > 0]

#дополнение контактов рекрутера из других вакансий компании
def pick_phone(phone_list):
    phones = [p for p in phone_list if pd.notna(p)]
    return phones[0] if phones else None

company_phone_map = (filtered.groupby('company')['recruiter_phone'].apply(lambda x: pick_phone(x.unique())))
filtered['recruiter_phone'] = filtered['recruiter_phone'].fillna(filtered['company'].map(company_phone_map))

filtered.to_csv("../../data_2/processed/final_set.csv", index=False)