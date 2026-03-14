import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from datetime import datetime
import re


BASE_URL = "https://www.work.ua"


def get_job_links(page=1):

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    url = f"https://www.work.ua/jobs-data+analyst/?page={page}"

    r = requests.get(url, headers=headers)

    print("status:", r.status_code)

    soup = BeautifulSoup(r.text, "lxml")

    links = set()

    for a in soup.select("a[href]"):

        href = a.get("href")

        if not href:
            continue

        # ищем /jobs/NUMBER/
        if re.match(r"^/jobs/\d+/?$", href):

            full_link = BASE_URL + href
            links.add(full_link)

    return list(links)


def fetch_job_page(url):

    headers = {
        "User-Agent": "Mozilla/5.0"
    }

    r = requests.get(url, headers=headers)

    if r.status_code != 200:
        print("Failed:", url)
        return None

    soup = BeautifulSoup(r.text, "lxml")

    time.sleep(1)

    return soup

def parse_job_page(soup):

    title = None
    company = None
    location = None

    # TITLE
    title_tag = soup.find("h1")

    if title_tag:
        title = title_tag.text.strip()

    # COMPANY
    for tag in soup.select("span.strong-500"):

        text = tag.text.replace("\xa0", " ").strip()

        if "грн" in text:
            continue

        company = text
        break

    # LOCATION
    location_icon = soup.select_one(".glyphicon-map-marker")

    location = (
        location_icon.parent.text.strip().split("\n")[0].split(",")[0].strip()
        if location_icon
        else "Remote")

    # SALARY
    salary_min = None
    salary_max = None

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

    # SKILLS
    skills = []

    for skill in soup.select("li.label-skill span"):
        skills.append(skill.text.strip())

    # EXPERIENCE + EDUCATION + TYPE
    employment_type = None
    experience_years = None
    education = None


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

    # DEFAULT VALUES
    employment_type = employment_type if employment_type else "Не вказано"
    experience_years = experience_years if experience_years is not None else 0
    education = education if education else "Не вказано"

    # PUBLISHED DATE
    time_tag = soup.select_one("time[datetime]")

    published_datetime = (time_tag["datetime"]
                          if time_tag
                          else datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    )
    return {"title": title,
            "company": company,
            "location": location,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "skills": skills,
            "employment_type": employment_type,
            "experience_years": experience_years,
            "education": education,
            "published_datetime": published_datetime}



def collect_job_links(pages=8):

    all_links = []

    for page in range(1, pages + 1):

        print(f"Collecting page {page}")

        links = get_job_links(page)

        print(f"Found {len(links)} jobs")

        all_links.extend(links)

    return all_links

# COLLECT LINKS
links = collect_job_links(8)

print(f"\nTotal links collected: {len(links)}")


# PARSE JOB PAGES
jobs_data = []

for i, link in enumerate(links):

    print(f"Parsing {i+1}/{len(links)}")

    soup = fetch_job_page(link)

    if not soup:
        continue

    job = parse_job_page(soup)

    job["url"] = link

    jobs_data.append(job)


print(f"\nParsed jobs: {len(jobs_data)}")


# CREATE DATAFRAME
df = pd.DataFrame(jobs_data)

print("\nDataset preview:")
print(df.head())

print("\nDataset shape:")
print(df.shape)


# SAVE DATASET
df.to_csv("../../data/csv/jobs_parsed16.csv", index=False)

print("\nCSV saved")