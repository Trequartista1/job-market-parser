import requests
import pandas as pd
import time
import re
from bs4 import BeautifulSoup

BASE_URL = "https://www.work.ua/jobs-data+analyst/"

headers = {"User-Agent": "Mozilla/5.0"}


def get_page(url):
    response = requests.get(url, headers=headers)
    print("Status:", response.status_code)
    return response.text


def parse_jobs(html):
    soup = BeautifulSoup(html, "lxml")
    jobs = soup.find_all("div", class_="card")
    print("Jobs found:", len(jobs))
    return jobs


def extract_job_data(jobs):

    jobs_data = []

    for job in jobs:

        title_tag = job.find("h2")
        link_tag = job.find("a")

        description_tag = job.find("p", class_="ellipsis")

        hot_tag = job.find("span", class_="label label-orange-light")

        # --- COMPANY ---
        company = None
        company_block = job.find("div", class_="mt-xs")

        if company_block:
            company_tag = company_block.find("span", class_="strong-600")
            if company_tag:
                company = company_tag.text.strip()

        # --- CITY ---
        city = None

        for span in job.find_all("span"):

            text = span.text.strip()

            if text.endswith(","):
                city = text.replace(",", "")
                break

        # --- SALARY ---
        salary = None
        salary_min = None
        salary_max = None

        for span in job.find_all("span", class_="strong-600"):

            text = span.text.strip()

            if "грн" in text:

                text = text.replace("\xa0", " ")
                text = text.replace("\u202f", " ")
                text = text.replace("\u2009", " ")

                salary = text

                numbers = re.findall(r"\d{1,3}(?:\s\d{3})*", text)
                numbers = [int(n.replace(" ", "")) for n in numbers]

                if len(numbers) == 1:
                    salary_min = numbers[0]

                elif len(numbers) >= 2:
                    salary_min = numbers[0]
                    salary_max = numbers[1]

        # --- DESCRIPTION CLEANING ---
        description_preview = None

        if description_tag:

            text = description_tag.text.strip()

            parts = text.split(".")

            if len(parts) > 3:
                description_preview = ".".join(parts[3:]).strip()
            else:
                description_preview = text
        # --- EXPERIENCE REQUIRED ---
        experience_required = None

        if description_tag:

            text = description_tag.text.lower()

            if "без досвіду" in text:
                experience_required = 0

            elif "1 року" in text or "1 рік" in text:
                experience_required = 1

            elif "2 років" in text or "2 роки" in text:
                experience_required = 2

            elif "3 років" in text or "3 роки" in text:
                experience_required = 3

            elif "5 років" in text:
                experience_required = 5

        # --- DATE POSTED ---
        date_posted = None

        if link_tag and "title" in link_tag.attrs:

            title_attr = link_tag["title"]

            if "вакансія від" in title_attr:
                date_posted = title_attr.split("вакансія від")[-1].strip()

        # --- JOB DATA ---
        if title_tag and link_tag:

            link = "https://www.work.ua" + link_tag["href"]

            job_id = link_tag["href"].split("/")[2]

            jobs_data.append({

                "job_id": job_id,
                "title": title_tag.text.strip(),
                "company": company,
                "city": city,
                "salary": salary,
                "salary_min": salary_min,
                "salary_max": salary_max,
                "date_posted": date_posted,
                "experience_required": experience_required,
                "description_preview": description_preview,
                "is_hot_job": True if hot_tag else False,
                "link": link
            })

    return jobs_data


def main():

    all_jobs = []

    for page in range(1, 10):

        print(f"Parsing page {page}")

        url = f"{BASE_URL}?page={page}"

        html = get_page(url)

        jobs = parse_jobs(html)

        jobs_data = extract_job_data(jobs)

        all_jobs.extend(jobs_data)

        time.sleep(1)

    df = pd.DataFrame(all_jobs)

    df = df.drop_duplicates(subset="job_id")

    print(df.head())

    df.to_csv("../data/final_jobs.csv", index=False)

    print(f"Saved {len(df)} jobs")


if __name__ == "__main__":
    main()