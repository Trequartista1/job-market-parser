import requests
from bs4 import BeautifulSoup
import time
import pandas as pd
from datetime import datetime
import re


BASE_URL = "https://www.work.ua"
SEARCH_QUERIES = ["data+analyst",
                  "business+analyst",
                  "marketing+analyst",
                  "financial+analyst",
                  "product+analyst",
                  "system+analyst",
                  "BI+analyst",
                  "data+scientist"]

# COLLECTING LINKS
def get_job_links(query, page=1):

    headers = {"User-Agent": "Mozilla/5.0"}
    url = f"https://www.work.ua/jobs-{query}/?page={page}"

    r = requests.get(url, headers=headers)
    soup = BeautifulSoup(r.text, "lxml")

    links = set()

    for a in soup.select("a[href]"):

        href = a.get("href")

        if not href:
            continue

        if re.match(r"^/jobs/\d+/?$", href):

            full_link = BASE_URL + href
            links.add(full_link)

    return list(links)

# COLLECTING HTML'S
def fetch_job_page(url):

    headers = {"User-Agent": "Mozilla/5.0"}
    r = requests.get(url, headers=headers)

    if r.status_code != 200:
        print("Failed:", url)
        return None

    soup = BeautifulSoup(r.text, "lxml")
    time.sleep(1.2)
    return soup

# PARSING HTML
def parse_job_page(soup, url, query):


    # TITLE
    title = None
    title_tag = soup.find("h1")

    if title_tag:
        title = title_tag.text.strip()

    # COMPANY
    company = "Компанія прихована"

    for tag in soup.select("span.strong-500"):

        text = tag.text.replace("\xa0", " ").strip()

        if "грн" in text:
            continue

        if text:
            company = text
            break

    # LOCATION
    location = None
    location_icon = soup.select_one(".glyphicon-map-marker")

    location = (location_icon.parent.text.strip().split("\n")[0].split(",")[0].strip()
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
                          else datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

    # CONTACTS
    recruiter_name = None
    recruiter_phone = None

    contact_block = soup.select_one("li.js-company-job-phone")

    if contact_block:

        name_span = contact_block.find("span", string=True)

        if name_span:
            recruiter_name = name_span.text.strip()

        phone_tag = contact_block.select_one('a[href^="tel:"]')

        if phone_tag:
            recruiter_phone = phone_tag.get("href").replace("tel:", "").strip()

    # COMPANY CONTACT LINKS
    company_links = []

    description_block = soup.select_one("#job-description")

    if description_block:

        for a in description_block.select("a[href]"):

            href = a.get("href")

            if not href:
                continue

            if "work.ua" in href:
                continue

            company_links.append(href)

    # FALLBACK CONTACT
    company_contact = None

    if recruiter_phone is None and company_links:

        for link in company_links:

            if "tel:" in link:
                company_contact = link.replace("tel:", "")
                break

            elif "http" in link:
                company_contact = link
                break

    return {"title": title,
            "company": company,
            "location": location,
            "salary_min": salary_min,
            "salary_max": salary_max,
            "skills": skills,
            "employment_type": employment_type,
            "experience_years": experience_years,
            "education": education,
            "published_datetime": published_datetime,
            "recruiter_name": recruiter_name,
            "recruiter_phone": recruiter_phone,
            "company_contact": company_contact,
            "link": url,
            "search_query": query.replace("+", " ")}



def collect_job_links(queries, pages=10):

    all_links = set()
    results = []

    for query in queries:

        print(f"\nSearch query: {query}")

        for page in range(1, pages + 1):

            print(f"Collecting page {page}")

            links = get_job_links(query, page)

            print(f"Found {len(links)} jobs")

            for link in links:

                if link not in all_links:
                    all_links.add(link)
                    results.append((link, query))

    return results

# COLLECT LINKS
links = collect_job_links(SEARCH_QUERIES, pages=20)

print(f"\nTotal links collected: {len(links)}")


# PARSE JOB PAGES
jobs_data = []

# MAIN LOOP
for i, (link, query) in enumerate(links):

    print(f"Parsing {i+1}/{len(links)}")

    soup = fetch_job_page(link)

    if not soup:
        continue

    job = parse_job_page(soup, link, query)

    jobs_data.append(job)


print(f"\nParsed jobs: {len(jobs_data)}")


# CREATE DATAFRAME
df = pd.DataFrame(jobs_data)

print("\nDataset preview:")
print(df.head())

print("\nDataset shape:")
print(df.shape)


# SAVE DATASET
df.to_csv("../../data/csv/jobs_dataset.csv", index=False)

print("\nCSV saved")