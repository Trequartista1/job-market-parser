import requests
import pandas as pd

GRAPHQL_URL = "https://dracula.robota.ua/graphql"


def fetch_vacancy(vacancy_id):

    headers = {
        "User-Agent": "Mozilla/5.0",
        "Content-Type": "application/json",
        "Referer": "https://robota.ua/",
        "x-apollo-operation-name": "getPublishedVacancy"
    }

    payload = {
        "operationName": "getPublishedVacancy",
        "variables": {
            "id": str(vacancy_id)
        },
        "query": """
        query getPublishedVacancy($id: ID!) {
          publishedVacancy(id: $id) {
            id
            title
            sortDate
            city { name }
            company { name }
            salary { amountFrom amountTo }
            contacts { name }
            keyTagGroups { name }
            schedules { name }
          }
        }
        """
    }

    r = requests.post(GRAPHQL_URL, headers=headers, json=payload)

    if r.status_code != 200:
        print("Failed:", vacancy_id, "status:", r.status_code)
        return None

    try:
        return r.json()
    except Exception:
        print("Not JSON:", vacancy_id)
        return None


def parse_vacancy(data):

    vacancy = data["data"]["publishedVacancy"]

    if vacancy is None:
        return None

    title = vacancy["title"]
    company = vacancy["company"]["name"]
    location = vacancy["city"]["name"]

    salary_min = vacancy["salary"]["amountFrom"]
    salary_max = vacancy["salary"]["amountTo"]

    published_datetime = vacancy["sortDate"]

    recruiter_name = None
    if vacancy.get("contacts"):
        recruiter_name = vacancy["contacts"]["name"]

    skills = []
    for tag in vacancy.get("keyTagGroups", []):
        skills.append(tag["name"])

    employment_type = []
    for s in vacancy.get("schedules", []):
        employment_type.append(s["name"])

    link = f"https://robota.ua/company0/vacancy{vacancy['id']}"

    return {
        "title": title,
        "company": company,
        "location": location,
        "salary_min": salary_min,
        "salary_max": salary_max,
        "skills": skills,
        "employment_type": employment_type,
        "published_datetime": published_datetime,
        "recruiter_name": recruiter_name,
        "link": link,
        "source": "robota.ua"
    }


if __name__ == "__main__":

    vacancy_ids = [
        11032786,
        11032785,
        11032780,
        11032770
    ]

    jobs = []

    for vid in vacancy_ids:

        print("Fetching:", vid)

        data = fetch_vacancy(vid)

        if not data:
            continue

        job = parse_vacancy(data)

        if job:
            jobs.append(job)

    df = pd.DataFrame(jobs)

    print("\nDataset preview:")
    print(df.head())

    print("\nDataset shape:")
    print(df.shape)

    df.to_csv("../data/csv/robota_jobs.csv", index=False)

    print("\nCSV saved")