import pandas as pd
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch
import numpy as np
from src.utils.logger_utils import setup_logger
from dotenv import load_dotenv
import os
from sqlalchemy import create_engine 
import logging
load_dotenv()

def read_data_from_db(table_name, driver, server, database, username, password):
    """
    Connect to SQL Server and read a table into a pandas DataFrame.
    """
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+{driver}+for+SQL+Server"
    engine = create_engine(conn_str)
    logging.info(f"Reading data from table: {table_name}")
    return pd.read_sql(table_name, con=engine)

def insert_into_sql_server(df, driver, server, database, username, password, mode, table_name):
    """
    Insert DataFrame into SQL Server table with specified mode (e.g., 'replace', 'append').
    """
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+{driver}+for+SQL+Server"
    engine = create_engine(conn_str)
    logging.info(f"Inserting data into table: {table_name} with mode: {mode}")
    df.to_sql(name=table_name, con=engine, schema='dbo', if_exists=mode, index=False)
    logging.info(f"Successfully inserted {len(df)} rows into {table_name}")

def predict_sentiment_review(text, tokenizer, model):
    """
    Predict sentiment class for a given text using a BERT model.
    Returns one of: 'Negative', 'Neutral', or 'Positive'.
    """
    if text is None or text is np.nan:
        text = ''
    inputs = tokenizer(text, return_tensors="pt", truncation=True, padding=True)
    with torch.no_grad():
        outputs = model(**inputs)
    probs = torch.nn.functional.softmax(outputs.logits, dim=-1)
    predicted_class = torch.argmax(probs).item()

    if predicted_class <= 2:
        return "Negative"
    elif predicted_class == 3:
        return "Neutral"
    else:
        return "Positive"

def add_sentiment_column(df, tokenizer, model):
    """
    Add a new 'Sentiment' column to DataFrame based on Title + Review text.
    """
    logging.info("Generating sentiment predictions...")
    df['All Text'] = "Title: " + df['Title'] + ", Review: " + df['Full Review']
    df['Sentiment'] = df['All Text'].apply(lambda x: predict_sentiment_review(x, tokenizer, model))
    logging.info("Sentiment column added.")
    return df

def main():
    logging.info("Starting sentiment enrichment pipeline...")

    model_name = "nlptown/bert-base-multilingual-uncased-sentiment"
    logging.info(f"Loading model: {model_name}")
    tokenizer = AutoTokenizer.from_pretrained(model_name)
    model = AutoModelForSequenceClassification.from_pretrained(model_name)

    # Load DB credentials
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    driver = 17
    table_name = "AIRLINE_REVIEW"

    # Read data
    df = read_data_from_db(table_name, driver, server, database, username, password)

    # Enrich data with sentiment
    enriched_df = add_sentiment_column(df, tokenizer, model)
    enriched_df.drop(['All Text'], axis=1, inplace=True)

    # Insert back into DB
    mode = 'replace'
    insert_into_sql_server(enriched_df, driver, server, database, username, password, mode, table_name)

    logging.info("Pipeline completed successfully.")

if __name__ == '__main__':
    main()