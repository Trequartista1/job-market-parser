import requests
import pandas as pd
import time
import re
from bs4 import BeautifulSoup


URL = "https://dracula.robota.ua/graphql"

HEADERS = {
    "User-Agent": "Mozilla/5.0",
    "Content-Type": "application/json",
    "Origin": "https://robota.ua",
    "Referer": "https://robota.ua/"
}


SEARCH_QUERIES = [
    "data analyst",
    "business analyst",
    "marketing analyst",
    "financial analyst",
    "product analyst",
    "system analyst",
    "bi analyst",
    "data scientist"
]


PAGE_SIZE = 20
MAX_PAGES = 25


# -------------------------------------------------
# SKILL MAP
# -------------------------------------------------

SKILL_MAP = {

    "python": ["python","python3"],
    "sql": ["sql","t-sql","postgresql","mysql","pl/sql"],

    "excel": ["excel","ms excel","microsoft excel"],
    "google sheets": ["google sheets","sheets"],

    "power bi": ["power bi","powerbi","power-bi"],
    "tableau": ["tableau"],
    "looker": ["looker","looker studio","google data studio"],

    "pandas": ["pandas"],
    "numpy": ["numpy"],

    "scikit-learn": ["scikit","scikit-learn","sklearn"],

    "tensorflow": ["tensorflow"],
    "pytorch": ["pytorch"],

    "spark": ["spark","pyspark","apache spark"],
    "hadoop": ["hadoop"],

    "airflow": ["airflow","apache airflow"],
    "dbt": ["dbt"],

    "bigquery": ["bigquery"],
    "snowflake": ["snowflake"],
    "redshift": ["redshift"],

    "aws": ["aws","amazon web services"],
    "gcp": ["gcp","google cloud"],
    "azure": ["azure"],

    "docker": ["docker"],
    "kubernetes": ["kubernetes","k8s"],

    "git": ["git","github","gitlab","bitbucket"],

    "jira": ["jira"],
    "confluence": ["confluence"],

    "statistics": ["statistics","statistical"],
    "hypothesis testing": ["hypothesis testing","ab test","a/b test"],

    "machine learning": ["machine learning","ml"],
    "deep learning": ["deep learning"],

    "nlp": ["nlp","natural language processing"],

    "etl": ["etl","data pipeline"],
    "data warehouse": ["data warehouse","dwh"],

    "api": ["api","rest api"],

    "linux": ["linux"],
    "bash": ["bash"]
}


# -------------------------------------------------
# SAFE GET
# -------------------------------------------------

def safe_get(obj, key):

    if obj is None:
        return None

    if isinstance(obj, dict):
        return obj.get(key)

    return None


# -------------------------------------------------
# SKILL EXTRACTION
# -------------------------------------------------

def extract_skills(html):

    soup = BeautifulSoup(html, "html.parser")

    text = soup.get_text(" ").lower()

    found = []

    for skill, variants in SKILL_MAP.items():

        for v in variants:

            if v in text:
                found.append(skill)
                break

    return list(set(found))


# -------------------------------------------------
# COLLECT IDS
# -------------------------------------------------

def collect_ids(query):

    ids = []

    print("\nSearching:", query)

    for page in range(MAX_PAGES):

        payload = {
            "operationName": "getPublishedVacanciesList",
            "variables": {
                "filter": {
                    "keywords": query,
                    "location": {
                        "longitude": 0,
                        "latitude": 0
                    }
                },
                "pagination": {
                    "count": PAGE_SIZE,
                    "page": page
                },
                "sort": "BY_BUSINESS_SCORE"
            },
            "query": """
            query getPublishedVacanciesList($filter: PublishedVacanciesFilterInput!, $pagination: PublishedVacanciesPaginationInput!, $sort: PublishedVacanciesSortType!) {
              publishedVacancies(filter: $filter, pagination: $pagination, sort: $sort) {
                totalCount
                items {
                  id
                  title
                }
              }
            }
            """
        }

        r = requests.post(URL, headers=HEADERS, json=payload)

        data = r.json()

        items = data["data"]["publishedVacancies"]["items"]

        if not items:
            break

        print("Page", page+1, ":", len(items))

        for v in items:
            ids.append(v["id"])

        time.sleep(0.4)

    print("Collected:", len(ids))

    return ids


# -------------------------------------------------
# GET VACANCY
# -------------------------------------------------

def get_vacancy(v_id):

    payload = {
        "operationName": "getPublishedVacancy",
        "variables": {"id": v_id},
        "query": """
        query getPublishedVacancy($id: ID!) {
          publishedVacancy(id: $id) {

            id
            title
            description
            fullDescription
            sortDateText

            company {
              name
            }

            city {
              name
            }

            salary {
              amountFrom
              amountTo
            }

            schedules {
              name
            }

            contacts {
              name
              phones
            }

          }
        }
        """
    }

    r = requests.post(URL, headers=HEADERS, json=payload)

    data = r.json()

    return safe_get(data.get("data"), "publishedVacancy")


# -------------------------------------------------
# MAIN
# -------------------------------------------------

def main():

    jobs = []

    for query in SEARCH_QUERIES:

        ids = collect_ids(query)

        print("\nDownloading vacancies\n")

        for v_id in ids:

            try:

                vacancy = get_vacancy(v_id)

                if not vacancy:
                    continue

                html = vacancy.get("fullDescription") or vacancy.get("description") or ""

                skills = extract_skills(html)

                schedules = vacancy.get("schedules")

                employment = None

                if schedules:

                    names = []

                    for s in schedules:

                        if s and s.get("name"):
                            names.append(s["name"])

                    if names:
                        employment = ",".join(names)

                contacts = vacancy.get("contacts")

                recruiter_name = None
                recruiter_phone = None

                if contacts:

                    recruiter_name = contacts.get("name")

                    phones = contacts.get("phones")

                    if phones:

                        recruiter_phone = str(phones[0]).replace(" ", "").replace("-", "")

                salary = vacancy.get("salary")

                salary_min = 0
                salary_max = 0

                if salary:

                    salary_min = salary.get("amountFrom") or 0
                    salary_max = salary.get("amountTo") or 0

                    if salary_min and not salary_max:
                        salary_max = salary_min

                job = {

                    "title": vacancy.get("title"),

                    "company": safe_get(vacancy.get("company"), "name"),

                    "location": safe_get(vacancy.get("city"), "name"),

                    "salary_min": salary_min,

                    "salary_max": salary_max,

                    "skills": skills,

                    "employment_type": employment,

                    "experience_years": None,

                    "education": None,

                    "published_datetime": vacancy.get("sortDateText"),

                    "recruiter_name": recruiter_name,

                    "recruiter_phone": recruiter_phone,

                    "link": f"https://robota.ua/company0/vacancy{v_id}",

                    "search_query": query,

                    "source": "robota.ua"
                }

                jobs.append(job)

                print(len(jobs), "parsed")

                time.sleep(0.35)

            except Exception as e:

                print("Error on vacancy", v_id)

                continue

    df = pd.DataFrame(jobs)

    df.drop_duplicates(subset=["link"], inplace=True)

    print("\nDataset shape:", df.shape)

    print(df.head())

    df.to_csv("robota_jobs.csv", index=False)

    print("\nSaved robota_jobs.csv")


if __name__ == "__main__":

    main()