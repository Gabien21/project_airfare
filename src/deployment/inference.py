import joblib
import pandas as pd
from unidecode import unidecode
import os
import numpy as np
from sqlalchemy import create_engine
from dotenv import load_dotenv

# ========================== Setup Path Constants ==========================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
MODEL_DIR = os.path.join(BASE_DIR, "models")
os.makedirs(MODEL_DIR, exist_ok=True)  

# ========================== Load Environment Variables ==========================
load_dotenv()
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")

# ========================== SQL Utilities ==========================
def get_engine():
    """Create and return a SQLAlchemy engine for SQL Server."""
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
    return create_engine(conn_str)

def get_code_from_sql(engine, column_return, table, column_match, value):
    """Query a specific value (code) from the SQL table."""
    query = f"SELECT {column_return} FROM {table} WHERE {column_match} = ?"
    result = pd.read_sql(query, engine, params=[(value,)])
    return result.iloc[0, 0] if not result.empty else None

engine = get_engine()

# ========================== Load Trained Model & Encoders ==========================
model = joblib.load(os.path.join(MODEL_DIR, "final_best_model.pkl"))
scaler = joblib.load(os.path.join(MODEL_DIR, "scalers_per_column.pkl"))
onehot_encoder = joblib.load(os.path.join(MODEL_DIR, "onehot_encoder.pkl"))
label_binarizer = joblib.load(os.path.join(MODEL_DIR, "multilabel_binarizer_refund_policy.pkl"))

# ========================== Preprocessing Functions ==========================
def feature_engineering_datetime(df):
    """Extract useful datetime features for the model."""
    df['Departure_Hour'] = df['Departure_Time'].dt.hour
    df['Departure_DayOfWeek'] = df['Departure_Time'].dt.dayofweek
    df['Days_Before_Departure'] = (df['Departure_Time'] - df['Scrape_Time']).dt.days
    return df

def preprocessing_input(df: pd.DataFrame):
    """Preprocess user input data to match model training format."""
    datetime_df = df[['Departure_Time', 'Scrape_Time']].copy()
    datetime_df['Departure_Time'] = pd.to_datetime(datetime_df['Departure_Time'])
    datetime_df['Scrape_Time'] = pd.to_datetime(datetime_df['Scrape_Time'])
    df.drop(columns=['Departure_Location_Code', 'Departure_Time', 'Scrape_Time'], inplace=True)

    # Numerical features
    num_col_df = df.select_dtypes(exclude=['object'])
    num_scaled = pd.DataFrame(index=df.index)
    for col in num_col_df.columns:
        num_scaled[col] = scaler[col].transform(df[[col]])[:, 0]

    # Categorical features (excluding multi-label)
    cat_col_df = df.select_dtypes(include=['object'])
    X_cat = onehot_encoder.transform(cat_col_df.drop(columns=['Refund_Policy']))
    encoded_columns = onehot_encoder.get_feature_names_out(cat_col_df.drop(columns=['Refund_Policy']).columns)
    X_cat_df = pd.DataFrame(X_cat, columns=encoded_columns, index=df.index)

    # Multi-label (Refund_Policy)
    onehot_df = pd.DataFrame(label_binarizer.transform(df["Refund_Policy"]),
                             columns=label_binarizer.classes_, index=df.index)

    # Datetime features
    datetime_df = feature_engineering_datetime(datetime_df)
    datetime_df.drop(columns=['Departure_Time', 'Scrape_Time'], inplace=True)

    # Combine all processed features
    final_df = pd.concat([num_scaled, X_cat_df, onehot_df, datetime_df], axis=1)
    final_df.columns = [unidecode(str(c)).strip().replace(" ", "_").replace(",", "") for c in final_df.columns]
    return final_df

# ========================== Prediction Function ==========================
def predict_airfare(df):
    """Predict the airfare based on processed input data."""
    return model.predict(df)

def predict_airfare_real(df):
    """Predict the airfare based on processed input data."""
    df["Airline_id"] = get_code_from_sql(engine, "Airline_id", "Airline", "Airline", df["Airline_id"].values[0])
    df["Arrival_Location_Code"] = get_code_from_sql(engine, "AirportCode", "Airport", "Location", df["Arrival_Location_Code"].values[0])
    df['Carry-on_Baggage'] = df['Carry-on_Baggage'].apply(lambda x: np.nan if x is None else x)
    df['Checked_Baggage'] = df['Checked_Baggage'].apply(lambda x: np.nan if x is None else x)
    X = preprocessing_input(df)
    y_pred = model.predict(X)
    pred_price = scaler['Total_Price'].inverse_transform(y_pred.reshape(1, -1))[0, 0]
    return pred_price

if __name__ == "__main__":
    sample_input = {
        "Carry-on_Baggage": 7,
        "Checked_Baggage": 20,
        "Flight_Duration": 1.5,
        "Fare_Class": "Economy",
        "Airline_id": "Vietnam Airlines",
        "Arrival_Location_Code": "Hà Nội",
        "Aircraft_Type": "Airbus A321",
        "Refund_Policy": ["Non-refundable"],
        "Departure_Time": "2025-06-22 09:00:00",
        "Scrape_Time": "2025-06-01 08:00:00",
        "Departure_Location_Code": None
    }

    print("predicted_price", int(predict_airfare_real(pd.DataFrame([sample_input]))))