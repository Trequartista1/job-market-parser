import pandas as pd

# -----------------------------
# LOAD DATASET
# -----------------------------
df = pd.read_csv("../../data_2/processed/final_set.csv")

# -----------------------------
# REMOTE FILTER
# -----------------------------

# work.ua -> location == Remote
workua_remote = df[
    (df["source"] == "workua") &
    (
        df["location"]
        .astype(str)
        .str.strip()
        .str.lower() == "remote"
    )
]

# robota.ua -> employment_type contains "віддал"
robotaua_remote = df[
    (df["source"] == "robota.ua") &
    (
        df["employment_type"]
        .astype(str)
        .str.lower()
        .str.contains("віддал", na=False)
    )
]

# merge
df = pd.concat(
    [workua_remote, robotaua_remote],
    ignore_index=True
)

# -----------------------------
# KEEP ONLY REQUIRED COLUMNS
# -----------------------------
df = df[
    [
        "title",
        "company",
        "recruiter_name",
        "recruiter_phone",
        "skills",
        "published_datetime",
        "link",
        "source"
    ]
]

# -----------------------------
# REMOVE EMPTY CONTACTS
# -----------------------------
df = df[df["recruiter_phone"].notna()]

# -----------------------------
# CLEAN CONTACTS
# -----------------------------
df["recruiter_phone"] = (
    df["recruiter_phone"]
    .astype(str)
    .str.strip()
)

# -----------------------------
# REMOVE INVALID CONTACTS
# -----------------------------
df = df[
    df["recruiter_phone"].str.startswith("t.me/+")
]

# -----------------------------
# REMOVE DUPLICATES
# -----------------------------
df = df.drop_duplicates(
    subset=["recruiter_phone"]
)

# -----------------------------
# SORT BY DATE
# -----------------------------
df["published_datetime"] = pd.to_datetime(
    df["published_datetime"],
    errors="coerce"
)

df = df.sort_values(
    "published_datetime",
    ascending=False
)

# -----------------------------
# LIMIT ROWS
# -----------------------------
df = df.head(100)

# -----------------------------
# RESET INDEX
# -----------------------------
df = df.reset_index(drop=True)

# -----------------------------
# SAVE
# -----------------------------
df.to_csv(
    "../../data_2/outreach/outreach_ready.csv",
    index=False
)

print(df.head())
print("\nRows:", len(df))

print("\noutreach_ready.csv saved")