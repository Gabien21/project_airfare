import pandas as pd
import numpy as np
import ast
import os
import joblib
import logging
from unidecode import unidecode
from dotenv import load_dotenv
from sqlalchemy import create_engine
import urllib
from sklearn.preprocessing import MultiLabelBinarizer, OneHotEncoder, StandardScaler
from src.utils.logger_utils import setup_logger

# ========================== Directory Setup ==========================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
DATA_DIR = os.path.join(BASE_DIR, "data")
MODEL_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(MODEL_DIR, exist_ok=True)

# ========================== Load Environment Variables ==========================
load_dotenv()
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")

# ========================== SQL Utilities ==========================
def load_data_from_sql(table_name):
    """Connect to SQL Server and read a table into a DataFrame."""
    logging.info(f"Loading table: {table_name}")
    params = urllib.parse.quote_plus(
        f"DRIVER=ODBC Driver 17 for SQL Server;"
        f"SERVER={server};"
        f"DATABASE={database};"
        f"UID={username};"
        f"PWD={password};"
        f"TrustServerCertificate=yes;"
        f"CHARSET=UTF8;"
    )
    conn_str = f"mssql+pyodbc:///?odbc_connect={params}"
    engine = create_engine(conn_str, fast_executemany=True)
    return pd.read_sql(table_name, engine)

def load_data():
    """Load and join all required tables from the SQL database."""
    logging.info("Joining all related tables from SQL Server...")
    airport_df = load_data_from_sql("AIRPORT")
    airline_df = load_data_from_sql("AIRLINE")
    refund_policy_df = load_data_from_sql("REFUND_POLICY")
    flight_schedule_df = load_data_from_sql("FLIGHT_SCHEDULE")
    ticket_df = load_data_from_sql("TICKET")

    df = ticket_df.merge(flight_schedule_df, on=['Departure Time', 'Flight Code', 'Departure Location Code'])
    df = df.merge(refund_policy_df, on=['Airline_id', 'Fare Class'])
    logging.info(f"Loaded {len(df)} rows after join.")
    return df, airport_df, airline_df, refund_policy_df, flight_schedule_df, ticket_df

# ========================== Data Cleaning ==========================
def handle_missing_value(df):
    logging.info("Handling missing values...")
    df['Checked_Baggage'] = df['Checked_Baggage'].fillna(0)
    return df

def handle_datetime(df):
    logging.info("Converting datetime columns...")
    df['Departure_Time'] = pd.to_datetime(df['Departure_Time'], errors='coerce')
    df['Arrival_Time'] = pd.to_datetime(df['Arrival_Time'], errors='coerce')
    df['Scrape_Time'] = pd.to_datetime(df['Scrape_Time'], errors='coerce')
    return df

def handle_numerical(df):
    logging.info("Dropping unnecessary numerical columns...")
    df.drop(columns=['Number_of_Tickets', "Price_per_Ticket", "Taxes_&_Fees"], inplace=True)
    return df

def handle_catrgorical(df):
    logging.info("Cleaning categorical features...")
    def handle_policy(x):
        if pd.isna(x): return []
        x = ast.literal_eval(x)
        return [y.replace("- ", "") for y in x]
    df.drop(columns=['Passenger_Type', 'Departure_Location_Code', 'Flight_Code'], inplace=True)
    df['Refund_Policy'] = df['Refund_Policy'].apply(handle_policy)
    return df

# ========================== Feature Engineering ==========================
def feature_engineering_datetime(df):
    logging.info("Generating datetime-based features...")
    df['Departure_Hour'] = df['Departure_Time'].dt.hour
    df['Departure_DayOfWeek'] = df['Departure_Time'].dt.dayofweek
    df['Days Before_Departure'] = (df['Departure_Time'] - df['Scrape_Time']).dt.days
    return df

def feature_engineering_numerical(df):
    logging.info("Scaling numerical features...")
    scalers = {}
    num_scaled = pd.DataFrame()
    for col in df.columns:
        scaler = StandardScaler()
        scaler.fit(df[[col]])
        num_scaled[col] = scaler.transform(df[[col]])[:, 0]
        scalers[col] = scaler
    joblib.dump(scalers, os.path.join(MODEL_DIR, "scalers_per_column.pkl"))
    return num_scaled

def feature_engineering_categorical(df):
    logging.info("Encoding categorical and multi-label features...")
    encoder = OneHotEncoder(sparse_output=False, handle_unknown='ignore')
    X_cat = encoder.fit_transform(df.drop(columns=['Refund_Policy']))
    encoded_columns = encoder.get_feature_names_out(df.drop(columns=['Refund_Policy']).columns)
    X_cat_df = pd.DataFrame(X_cat, columns=encoded_columns, index=df.index)
    joblib.dump(encoder, os.path.join(MODEL_DIR, "onehot_encoder.pkl"))

    mlb = MultiLabelBinarizer()
    mlb.fit(df["Refund_Policy"])
    one_hot = pd.DataFrame(mlb.transform(df['Refund_Policy']),
                           columns=mlb.classes_, index=df.index)
    joblib.dump(mlb, os.path.join(MODEL_DIR, "multilabel_binarizer_refund_policy.pkl"))

    return pd.concat([X_cat_df, one_hot], axis=1).astype(np.uint8)

def feature_engineering(df):
    """Combine all engineered features into one DataFrame."""
    logging.info("Combining all feature types...")
    datetime_col_df = feature_engineering_datetime(df.select_dtypes(include='datetime').copy())
    num_col_df = feature_engineering_numerical(df.select_dtypes(exclude=['datetime64[ns]', 'object']).copy())
    cat_col_df = feature_engineering_categorical(df.select_dtypes(include='object').copy())

    df = pd.concat([
        num_col_df.drop(columns=['Total_Price']),
        cat_col_df,
        datetime_col_df.drop(columns=["Departure_Time", "Scrape_Time", "Arrival_Time"]),
        num_col_df['Total_Price']
    ], axis=1)
    logging.info("Feature engineering completed.")
    return df

# ========================== Transformation Pipeline ==========================
def transform(df):
    """Full preprocessing pipeline to clean, parse, and engineer features."""
    logging.info("Starting transformation pipeline...")
    df = handle_missing_value(df)
    df = handle_datetime(df)
    num_col_df = handle_numerical(df.select_dtypes(exclude=['datetime64[ns]', 'object']).copy())
    cat_col_df = handle_catrgorical(df.select_dtypes(include='object').copy())
    df = pd.concat([num_col_df, cat_col_df, df.select_dtypes(include=['datetime64[ns]'])], axis=1)
    return feature_engineering(df)

# ========================== Main Entry Point ==========================
def preprocess_for_modeling():
    """Main function to extract and preprocess data, saving the result to CSV."""
    logging.info("Starting preprocessing for model training...")
    df, *_ = load_data()
    
    logging.info("Normalizing column names...")
    df.columns = [unidecode(c).strip("- ").strip().replace(" ", "_").replace(",", "") for c in df.columns]
    
    df = transform(df)

    os.makedirs(os.path.join(DATA_DIR, "data_for_modeling"), exist_ok=True)
    output_path = os.path.join(DATA_DIR, "data_for_modeling", "data.csv")
    df.to_csv(output_path, index=False)
    logging.info(f"Final dataset saved to: {output_path}")

if __name__ == "__main__":
    setup_logger(log_dir="logs")
    preprocess_for_modeling()
