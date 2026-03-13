from pathlib import Path
import pandas as pd
import re
from bs4 import BeautifulSoup

# путь к папке с html
BASE_DIR = Path(__file__).resolve().parents[2]
HTML_DIR = BASE_DIR / "data" / "html_files" / "raw_files"

jobs_data = []

# перебираем html файлы
for file in HTML_DIR.glob("*.html"):

    with open(file, encoding="utf-8") as f:
        soup = BeautifulSoup(f.read(), "lxml")

    # Data to store
    title = None
    company = None
    location = None
    salary_min = None
    salary_max = None
    employment_type = None
    experience_years = None
    education = None
    skills = []

    # TITLE
    title_tag = soup.find("h1")

    if title_tag:
        title = title_tag.text.strip()

    # COMPANY
    for tag in soup.select("span.strong-500"):

        text = tag.text.replace("\xa0", " ").strip()

        # пропускаем зарплату
        if "грн" in text:
            continue

        company = text
        break

    # LOCATION
    location_icon = soup.select_one(".glyphicon-map-marker")

    location = (
        ",".join(location_icon.parent.text.strip().split("\n")[0].split(",")[:3]).replace(".", "").strip()
        if location_icon
        else "Remote"
    )

    # SKILLS
    for skill in soup.select("li.label-skill span"):
        skills.append(skill.text.strip())

    # SALARY
    salary_icon = soup.select_one(".glyphicon-hryvnia-fill")

    if salary_icon:

        salary_block = salary_icon.find_parent("li")
        salary_tag = salary_block.select_one(".strong-500")

        if salary_tag:

            salary_text = salary_tag.text.strip()

            nums = re.findall(r"\d[\d\s\u202f\u2009]*", salary_text)

            nums = [int(re.sub(r"\D", "", n)) for n in nums]

            if len(nums) == 1:
                salary_min = salary_max = nums[0]

            elif len(nums) >= 2:
                salary_min = nums[0]
                salary_max = nums[1]

    # EXPERIENCE + EDUCATION + TYPE
    conditions_icon = soup.select_one('span[title="Умови й вимоги"]')

    if conditions_icon:

        conditions_block = conditions_icon.find_parent("li")

        conditions_text = " ".join(conditions_block.get_text().split())

        parts = [p.strip() for p in conditions_text.split(".") if p.strip()]

        for part in parts:
            part_lower = part.lower()

            if "зайнятість" in part_lower:
                employment_type = part

            elif "досвід" in part_lower:

                if "без досвіду" in part_lower:
                    experience_years = 0

                else:
                    nums = re.findall(r"\d+", part)
                    experience_years = int(nums[0]) if nums else 0

            elif "освіта" in part_lower:
                education = part

    # значения по умолчанию
    employment_type = employment_type if employment_type else "Не вказано"
    experience_years = experience_years if experience_years is not None else 0
    education = education if education else "Не вказано"

    jobs_data.append({
        "title": title,
        "company": company,
        "location": location,
        "skills": skills,
        "employment_type": employment_type,
        "experience_years": experience_years,
        "education": education,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "file": file.name
    })

# dataframe
df = pd.DataFrame(jobs_data)

# frame export
df.to_csv("../../data/csv/jobs_parsed12.csv", index=False)

print(df.head())
print(df.shape)