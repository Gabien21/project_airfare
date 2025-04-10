import os
import re
import json
import pandas as pd
import numpy as np
import logging
from datetime import datetime
from dotenv import load_dotenv
from src.utils.logger_utils import setup_logger
from sqlalchemy import create_engine
import sqlalchemy

# Load environment variables
load_dotenv()

# Paths
RAW_PATH = "data/raw"
CLEAN_PATH = "data/clean"
os.makedirs(CLEAN_PATH, exist_ok=True)


# ---------------------------- TEXT DATA FUNCTIONS ---------------------------- #

def _extract_field(pattern, text):
    """Extract a single match from text using regex pattern."""
    match = re.search(pattern, text)
    return match.group(1) if match else None


def extract_info(original_path, file_name, airline_dict):
    """
    Extracts structured fields from a raw airline information text file 
    and appends it to the provided dictionary.
    """
    logging.info(f"🔍 Processing airline info file: {file_name}")

    with open(f"{original_path}/{file_name}", "r") as file:
        all_text = file.read()

    airline_dict["name"].append(_extract_field(r"Name: (.+)\n", all_text))
    airline_dict["phone"].append(_extract_field(r"Phone: ([\d\s]+)\n", all_text))
    airline_dict["address"].append(_extract_field(r"Headquarters: (.+)\n", all_text))
    airline_dict["website"].append(_extract_field(r"Website: (.+)\n", all_text))

    avg_rating = re.search(r"Average Rating: ([\d.]+)\n", all_text)
    airline_dict["averating_rating"].append(float(avg_rating.group(1)) if avg_rating else None)

    total_review = re.search(r"Total Review: ([\d,]+) reviews\n", all_text)
    airline_dict["total_review"].append(int(total_review.group(1).replace(',', '')) if total_review else None)

    mentions = re.search(r"Popular Mention: \[(.+)\]", all_text)
    airline_dict["popular_mention"].append(
        re.sub("'", "", mentions.group(1)).split(", ") if mentions else []
    )

    attributes = re.search(r"Attributes: (.+)", all_text)
    airline_dict["attributes"].append(
        json.loads(attributes.group(1).replace("'", "\"")) if attributes else {}
    )

    rating = re.search(r"Total Rating: (.+)", all_text)
    airline_dict["rating"].append(
        json.loads(rating.group(1).replace("'", "\"")) if rating else {}
    )

    logging.debug(f"✅ Extracted fields for {file_name}")


def create_mention_df(airline_df):
    """
    Normalize mentions into a separate mention table.
    """
    logging.info("🧩 Creating Mention table...")
    df = airline_df[['name', 'popular_mention']].copy()
    df['airline_id'] = df.index
    df = df.explode('popular_mention', ignore_index=True)
    df.drop(columns='name', inplace=True)
    return df


def create_rating_df(airline_df):
    """
    Normalize ratings into a structured rating table.
    """
    logging.info("🧩 Creating Rating table...")
    df = airline_df[['name', 'rating']].copy()
    df['airline_id'] = df.index
    df.drop(columns='name', inplace=True)

    sub_dfs = [
        pd.DataFrame([{
            'airline_id': row['airline_id'],
            'rate_name': k,
            'count': v
        } for k, v in row['rating'].items()])
        for _, row in df.iterrows()
    ]

    return pd.concat(sub_dfs, ignore_index=True)


def create_attribute_df(airline_df):
    """
    Normalize attribute ratings into structured attribute table.
    """
    logging.info("🧩 Creating Attribute table...")
    df = airline_df[['name', 'attributes']].copy()
    df['airline_id'] = df.index
    df.drop(columns='name', inplace=True)

    sub_dfs = [
        pd.DataFrame([{
            'airline_id': row['airline_id'],
            'attribute_name': k,
            'rating': v
        } for k, v in row['attributes'].items()])
        for _, row in df.iterrows()
    ]

    final_df = pd.concat(sub_dfs, ignore_index=True)
    final_df['rating'] = final_df['rating'].apply(
        lambda x: float(re.search(r"(\d\.\d) of", x).group(1)) if isinstance(x, str) else None
    )
    return final_df


def process_airline_data():
    """
    Pipeline for extracting and transforming airline information data.
    """
    logging.info("🚀 Starting to process airline metadata...")
    info_headers = ['name', 'phone', 'address', 'website', 'averating_rating',
                    'total_review', 'popular_mention', 'attributes', 'rating']
    airline_dict = {k: [] for k in info_headers}

    for file in ["vj_general_info.txt", "vna_general_info.txt", "bamboo_general_info.txt"]:
        extract_info(RAW_PATH, file, airline_dict)

    airline_df = pd.DataFrame(airline_dict)
    airline_info_df = airline_df[['name', 'phone', 'address', 'website', 'averating_rating', 'total_review']].copy()

    mention_df = create_mention_df(airline_df)
    rating_df = create_rating_df(airline_df)
    attribute_df = create_attribute_df(airline_df)

    logging.info("✅ Airline metadata processing complete.")
    return airline_info_df, mention_df, rating_df, attribute_df


# ---------------------------- REVIEW DATA FUNCTIONS ---------------------------- #

def extract_airline_review():
    """
    Loads raw review CSV files for all airlines.
    """
    logging.info("📥 Extracting airline review data...")
    f"{RAW_PATH}/bamboo_all_reviews_data.csv"
    vj_review_df = pd.read_csv(f"{RAW_PATH}/vj_all_reviews_data.csv")
    vna_review_df = pd.read_csv(f"{RAW_PATH}/vna_all_reviews_data.csv")
    bam_review_df = pd.read_csv(f"{RAW_PATH}/bamboo_all_reviews_data.csv")

    return vj_review_df, vna_review_df, bam_review_df


def add_airline_name_column(df, name):
    """Adds 'Airline' column to DataFrame."""
    df['Airline'] = name
    return df


def merge_all_airlines(*dfs):
    """Merges multiple airline DataFrames into one."""
    return pd.concat(dfs, ignore_index=True)


def extract_rating_column(df):
    """Extracts numeric rating from string."""
    df['Rating'] = df['Rating'].apply(lambda x: float(re.search(r"(\d\.\d) of", x).group(1)))
    return df


def extract_date_information(df):
    """Extracts and converts travel date information from review text."""
    df.dropna(subset=['Information'], inplace=True)
    df['Information'] = df['Information'].apply(
        lambda x: datetime.strptime(re.search(r"Date of travel: (.+)", x).group(1), "%B %Y")
    )
    return df


def preprocess(text):
    """Lowercases, strips, and removes punctuation from text."""
    if not isinstance(text, str):
        return ''
    text = re.sub(r'\s+', ' ', text.lower())
    text = re.sub(r'[^\w\s]', '', text)
    return text


def preprocess_text_information(df):
    """Preprocesses Title and Full Review fields."""
    df['Title'] = df['Title'].apply(preprocess)
    df['Full Review'] = df['Full Review'].apply(preprocess)
    return df


def create_service_rating(df):
    """
    Extracts service-level rating breakdown from JSON in each review.
    """
    logging.info("🧩 Creating Service Rating table...")
    service_df = df[['airline_id', 'Service Ratings', 'Information']].copy()
    service_df.dropna(subset=['Service Ratings'], inplace=True)

    service_df['Service Ratings'] = service_df['Service Ratings'].apply(
        lambda x: json.loads(x.replace("'", "\""))
    )

    sub_dfs = [
        pd.DataFrame([{
            'airline_id': row['airline_id'],
            'service_name': s['Service Info'],
            'rating': s['Service Rating']
        } for s in row['Service Ratings']])
        for _, row in service_df.iterrows()
    ]

    final_df = pd.concat(sub_dfs, ignore_index=True)
    final_df['rating'] = final_df['rating'].apply(
        lambda x: float(re.search(r"(\d\.\d) of", x).group(1)) if isinstance(x, str) else None
    )
    return final_df


def clean_review_data(df):
    """Cleans and transforms review DataFrame."""
    df = extract_rating_column(df)
    df = extract_date_information(df)
    df = preprocess_text_information(df)
    return df


def process_airline_review(vj, vna, bam):
    """
    Complete pipeline to clean and prepare airline review data.
    """
    logging.info("🚀 Starting review data processing pipeline...")
    vj = add_airline_name_column(vj, 'VietJetAir')
    vna = add_airline_name_column(vna, 'Vietnam Airlines')
    bam = add_airline_name_column(bam, 'Bamboo Airways')

    full_df = merge_all_airlines(vj, vna, bam)
    full_df = clean_review_data(full_df)
    full_df['airline_id'] = full_df.groupby('Airline').ngroup()

    service_df = create_service_rating(full_df)

    full_df.drop(columns=['Service Ratings', 'Airline'], inplace=True)

    logging.info("✅ Airline review processing complete.")
    return full_df, service_df


def insert_into_sql_server(df, driver, server, database, username, password, mode, table_name):
    """
    Inserts a DataFrame into a SQL Server table using SQLAlchemy.
    """
    logging.info(f"💾 Inserting data into SQL Server table: {table_name}")
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+{driver}+for+SQL+Server"
    engine = create_engine(conn_str)

    dtype = {col: sqlalchemy.types.NVARCHAR(length=1000) for col in df.select_dtypes(include='object').columns}
    df.to_sql(name=table_name, con=engine, schema='dbo', if_exists=mode, index=False, dtype=dtype)

    logging.info(f"✅ Data inserted into table '{table_name}'.")


def load(airline_info_df, mention_df, rating_df, attribute_df, full_df, service_df):
    """
    Loads data to cleaned CSV files and SQL Server.
    """
    logging.info("💾 Saving and loading data to destination...")

    airline_info_path = os.path.join(CLEAN_PATH, "info.csv")
    airline_info_df.to_csv(airline_info_path, index=False)

    full_review_path = os.path.join(CLEAN_PATH, "all_airlines_review_cleaned.csv")
    full_df.to_csv(full_review_path, index=False)

    output_dir = os.path.join(CLEAN_PATH, "review_airline")
    os.makedirs(output_dir, exist_ok=True)

    mention_df.to_csv(os.path.join(output_dir, "mention.csv"), index=False)
    rating_df.to_csv(os.path.join(output_dir, "rating.csv"), index=False)
    attribute_df.to_csv(os.path.join(output_dir, "attribute.csv"), index=False)
    service_df.to_csv(os.path.join(output_dir, "review_service.csv"), index=False)

    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    driver = 17
    mode = 'replace'

    insert_into_sql_server(mention_df, driver, server, database, username, password, mode, "MENTION")
    insert_into_sql_server(rating_df, driver, server, database, username, password, mode, "RATING")
    insert_into_sql_server(full_df, driver, server, database, username, password, mode, "AIRLINE_REVIEW")
    insert_into_sql_server(airline_info_df, driver, server, database, username, password, mode, "INFO")
    insert_into_sql_server(attribute_df, driver, server, database, username, password, mode, "ATTRIBUTE")
    insert_into_sql_server(service_df, driver, server, database, username, password, mode, "REVIEW_SERVICE")


def main():
    """Main ETL entry point."""
    setup_logger(log_dir="logs")
    logging.info("🛫 === STARTING ETL PROCESS ===")

    try:
        airline_info_df, mention_df, rating_df, attribute_df = process_airline_data()
        vj_review_df, vna_review_df, bam_review_df = extract_airline_review()
        full_df, service_df = process_airline_review(vj_review_df, vna_review_df, bam_review_df)
        load(airline_info_df, mention_df, rating_df, attribute_df, full_df, service_df)
        logging.info("✅ === ETL PROCESS COMPLETED SUCCESSFULLY ===")

    except Exception as e:
        logging.exception("❌ ETL process failed due to an error.")


if __name__ == "__main__":
    main()

# python -m src.etl.preprocessing_airline_review