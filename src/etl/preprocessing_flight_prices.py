import os
import pandas as pd
import numpy as np
import logging
import ast
from dotenv import load_dotenv
from src.utils.logger_utils import setup_logger
from sqlalchemy import create_engine
import sqlalchemy

# Load environment variables from .env file
load_dotenv()

# Define input/output paths from environment variables
RAW_PATH = os.getenv("RAW_PATH", "data/raw")
CLEAN_PATH = os.getenv("CLEAN_PATH", "data/clean")
os.makedirs(CLEAN_PATH, exist_ok=True)

def extract():
    """
    Extract raw flight data from CSV files for routes SGN to HAN and SGN to DAD.
    """
    logging.info("📥 Extracting data from source files...")
    df_to_han = pd.read_csv(os.path.join(RAW_PATH, "flight_prices_SGN_to_HAN.csv"))
    df_to_dad = pd.read_csv(os.path.join(RAW_PATH, "flight_prices_SGN_to_DAD.csv"))
    logging.debug(f"Loaded {len(df_to_han)} rows from SGN to HAN")
    logging.debug(f"Loaded {len(df_to_dad)} rows from SGN to DAD")
    return df_to_han, df_to_dad

def merge_flight_routes(df1, df2):
    """
    Merge flight data from two routes into a single DataFrame.
    """
    return pd.concat([df1, df2], ignore_index=True)

def extract_ticket_details(df):
    """
    Extract airline, flight code, and fare class from ticket description string.
    """

    def parse_ticket_string(ticket_str):
        try:
            parts = ticket_str.split("Chuyến bay:")
            airline = parts[0].strip()
            flight_code = parts[1].split("Hạng vé :")[0].strip()
            fare_class = parts[1].split("Hạng vé :")[1].strip()
            return airline, flight_code, fare_class
        except:
            return None, None, None

    df[['Airline', 'Flight Code', 'Fare Class']] = df['Ticket Price'].apply(
        lambda row: pd.Series(parse_ticket_string(row))
    )
    df.drop(columns=['Ticket Price'], inplace=True)
    return df

def clean_price_columns(df):
    """
    Clean currency fields and convert to integers.
    """

    def clean_currency(value):
        if pd.isna(value):
            return None
        return int(value.split("VNĐ")[0].replace(",", "").strip())

    for col in ['Price per Ticket', 'Taxes & Fees', 'Total Price']:
        df[col] = df[col].apply(clean_currency)

    return df

def convert_time_columns(df):
    """
    Convert time-related columns to datetime.
    """
    df['Departure Time'] = pd.to_datetime(df['Departure Time'], dayfirst=True)
    df['Arrival Time'] = pd.to_datetime(df['Arrival Time'], dayfirst=True)
    df['Scrape Time'] = pd.to_datetime(df['Scrape Time'])
    return df

def clean_flight_metadata(df):
    """
    Clean aircraft type and convert flight duration to hours.
    """

    def clean_aircraft_type(x):
        if pd.isna(x):
            return None
        return x.replace("Máy bay:", "").replace("(máy bay lớn)", "").strip()

    def convert_flight_duration_to_hour(x):
        if pd.isna(x):
            return None
        parts = x.split("giờ")
        hour = float(parts[0].strip())
        minute = float(parts[1].replace("phút", "").strip())
        return np.round(hour + minute / 60, 2)

    df['Aircraft Type'] = df['Aircraft Type'].apply(clean_aircraft_type)
    df['Flight Duration'] = df['Flight Duration'].apply(convert_flight_duration_to_hour)
    return df

def parse_baggage_info(df):
    """
    Clean baggage info: carry-on and checked baggage.
    """

    def parse_carry_on(x):
        if pd.isna(x):
            return None
        if "x" in x:
            return 18
        return int(x.replace("kg", ""))

    def parse_checked(x):
        if pd.isna(x) or x == "Vui lòng chọn ở bước tiếp theo":
            return None
        return int(x.replace("kg", ""))

    df['Carry-on Baggage'] = df['Carry-on Baggage'].apply(parse_carry_on)
    df['Checked Baggage'] = df['Checked Baggage'].apply(parse_checked)
    return df

def parse_refund_policy(df):
    """
    Parse refund policy string using literal_eval.
    """
    import ast

    def safe_eval(x):
        if pd.isna(x):
            return []
        try:
            return ast.literal_eval(x)
        except:
            return []

    # df['Refund Policy'] = df['Refund Policy'].apply(safe_eval)
    df['Refund Policy'].apply(safe_eval)
    return df


def normalize_location_columns(df):
    """
    Split location column into location name and airport code.
    """

    def extract_code(location):
        if pd.isna(location):
            return None
        return location.split("(")[1].replace(")", "").strip()

    def extract_name(location):
        if pd.isna(location):
            return None
        return location.split("(")[0].strip()

    df['Departure Location Code'] = df['Departure Location'].apply(extract_code)
    df['Departure Location'] = df['Departure Location'].apply(extract_name)
    df['Arrival Location Code'] = df['Arrival Location'].apply(extract_code)
    df['Arrival Location'] = df['Arrival Location'].apply(extract_name)

    return df


def clean_data(df_to_han, df_to_dad):
    """
    Perform data cleaning steps:
    - Merge routes
    - Extract airline, flight code, fare class
    - Clean prices, time columns, aircraft, baggage, refund, and locations
    """
    logging.info("🧹 Starting transformation pipeline (cleaning)...")

    df = merge_flight_routes(df_to_han, df_to_dad)
    logging.debug(f"Merged total rows: {len(df)}")

    df = extract_ticket_details(df)
    logging.debug("Extracted airline, flight code, fare class.")

    df = clean_price_columns(df)
    logging.debug("Cleaned price-related columns.")

    df = convert_time_columns(df)
    logging.debug("Converted time columns to datetime.")

    df = clean_flight_metadata(df)
    logging.debug("Cleaned aircraft type and flight duration.")

    df = parse_baggage_info(df)
    logging.debug("Parsed carry-on and checked baggage.")

    df = parse_refund_policy(df)
    logging.debug("Parsed refund policy (from string to list).")

    df = normalize_location_columns(df)
    logging.debug("Split airport locations into names and codes.")

    return df


def normalize_tables(df):
    """
    Normalize raw flat DataFrame into multiple dimension and fact tables:
    - airport_df: Dimension table for airports
    - airline_df: Dimension table for airlines
    - refund_policy_df: Dimension table for refund policy per airline & fare class
    - flight_schedule_df: Dimension table for individual flight schedule
    - ticket_df: Fact table for tickets and pricing
    """
    logging.info("📊 Normalizing data into schema tables...")


    logging.debug("Generating AIRPORT table...")
    dep_airports = df[['Departure Location Code', 'Departure Location']].drop_duplicates()
    dep_airports.columns = ['AirportCode', 'Location']

    arr_airports = df[['Arrival Location Code', 'Arrival Location']].drop_duplicates()
    arr_airports.columns = ['AirportCode', 'Location']

    airport_df = pd.concat([dep_airports, arr_airports], ignore_index=True).drop_duplicates().reset_index(drop=True)


    logging.debug("Generating AIRLINE table...")
    airline_df = df[['Airline']].drop_duplicates().reset_index(drop=True)
    airline_df['Airline_id'] = "AL" + (airline_df.index + 1).astype(str).str.zfill(3)
    airline_df = airline_df[['Airline_id', 'Airline']]

    tmp_df = df.drop(columns=['Departure Location', 'Arrival Location', 'Refund Policy']).drop_duplicates()
    tmp_df = tmp_df.merge(airline_df, on='Airline', how='left').drop(columns='Airline')


    logging.debug("Generating REFUND_POLICY table...")
    refund_policy_df = df[['Airline', 'Fare Class', 'Refund Policy']].drop_duplicates()
    refund_policy_df = refund_policy_df.merge(airline_df, on='Airline', how='left').drop(columns='Airline')
    refund_policy_df = refund_policy_df[['Airline_id', 'Fare Class', 'Refund Policy']].reset_index(drop=True)


    logging.debug("Generating FLIGHT_SCHEDULE table...")
    flight_schedule_df = tmp_df[[
        'Departure Time', 'Flight Code', 'Departure Location Code',
        'Arrival Location Code', 'Flight Duration', 'Arrival Time', 'Aircraft Type'
    ]].drop_duplicates().reset_index(drop=True)


    logging.debug("Generating TICKET table...")
    ticket_df = tmp_df.drop(columns=[
        'Departure Time', 'Flight Code', 'Flight Duration', 'Arrival Time', 'Aircraft Type'
    ]).drop_duplicates().reset_index(drop=True)

    return airport_df, airline_df, refund_policy_df, flight_schedule_df, ticket_df


def transform(df_to_han, df_to_dad):
    """
    Transform raw flight data into normalized schema:
    - Clean data
    - Generate normalized dimension & fact tables
    """
    logging.info("🔄 Starting transformation process...")
    df = clean_data(df_to_han, df_to_dad)
    airport_df, airline_df, refund_policy_df, flight_schedule_df, ticket_df = normalize_tables(df)

    logging.info("✅ Transformation completed.")
    return df, airport_df, airline_df, refund_policy_df, flight_schedule_df, ticket_df


def insert_into_sql_server(df, driver, server, database, username, password, mode, table_name):
    """
    Insert a DataFrame into SQL Server using SQLAlchemy engine.
    """
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+{driver}+for+SQL+Server"
    engine = create_engine(conn_str)

    dtype = {}
    for col in df.columns:
        if df[col].dtype == 'object':
            dtype[col] = sqlalchemy.types.NVARCHAR(length=1000)

    df.to_sql(name=table_name, con=engine, schema='dbo', if_exists=mode, index=False, dtype=dtype)
    logging.info(f"Inserted table '{table_name}' into SQL Server.")


def load(df, airport_df, airline_df, refund_policy_df, flight_schedule_df, ticket_df):
    """
    Load cleaned data to CSV files and insert into SQL Server database.
    """

    # Save combined cleaned data to CSV
    combined_output_path = os.path.join(CLEAN_PATH, "flight_prices_combined_cleaned.csv")
    df.to_csv(combined_output_path, index=False)
    logging.info(f"Saved combined cleaned data to: {combined_output_path}")

    # Create output directory
    output_dir = os.path.join(CLEAN_PATH, "flight_prices")
    os.makedirs(output_dir, exist_ok=True)

    # Save individual dimension/fact tables to CSV
    airport_df.to_csv(os.path.join(output_dir, "airport.csv"), index=False)
    airline_df.to_csv(os.path.join(output_dir, "airline.csv"), index=False)
    refund_policy_df.to_csv(os.path.join(output_dir, "refund_policy.csv"), index=False)
    flight_schedule_df.to_csv(os.path.join(output_dir, "flight_schedule.csv"), index=False)
    ticket_df.to_csv(os.path.join(output_dir, "ticket.csv"), index=False)

    logging.info(f"Saved all normalized tables to: {output_dir}")

    # Load to SQL Server
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    driver = 17
    mode = 'replace'  # Options: 'fail', 'replace', 'append'

    insert_into_sql_server(airport_df, driver, server, database, username, password, mode, "AIRPORT")
    insert_into_sql_server(airline_df, driver, server, database, username, password, mode, "AIRLINE")
    insert_into_sql_server(refund_policy_df, driver, server, database, username, password, mode, "REFUND_POLICY")
    insert_into_sql_server(flight_schedule_df, driver, server, database, username, password, mode, "FLIGHT_SCHEDULE")
    insert_into_sql_server(ticket_df, driver, server, database, username, password, mode, "TICKET")


def main():
    """
    Main ETL execution flow:
    - Extract raw flight data
    - Transform and normalize
    - Load to CSV and SQL Server
    """
    setup_logger(log_dir="logs")
    logging.info("=== STARTING ETL PROCESS ===")

    try:
        # Extract
        df_to_han, df_to_dad = extract()

        # Transform
        df, airport_df, airline_df, refund_policy_df, flight_schedule_df, ticket_df = transform(df_to_han, df_to_dad)

        # Load
        load(df, airport_df, airline_df, refund_policy_df, flight_schedule_df, ticket_df)

        logging.info("=== ETL PROCESS COMPLETED SUCCESSFULLY ===")
        
    except Exception as e:
        logging.exception("ETL process failed due to an error.")


if __name__ == "__main__":
    main()
