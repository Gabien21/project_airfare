from fastapi import FastAPI
from pydantic import BaseModel
import pandas as pd
from ..deployment.inference import predict_airfare_real

app = FastAPI()

# Input schema
class FlightInput(BaseModel):
    Carry_on_Baggage: float
    Checked_Baggage: float
    Flight_Duration: float
    Fare_Class: str
    Airline: str
    Arrival_Location: str
    Aircraft_Type: str
    Refund_Policy: list[str]
    Departure_Time: str
    Scrape_Time: str
    Departure_Location: str

@app.post("/predict")
def predict_price(data: FlightInput):
    input_df = pd.DataFrame([{
        "Carry-on_Baggage": data.Carry_on_Baggage,
        "Checked_Baggage": data.Checked_Baggage,
        "Flight_Duration": data.Flight_Duration,
        "Fare_Class": data.Fare_Class,
        "Airline_id": data.Airline,
        "Arrival_Location_Code": data.Arrival_Location,
        "Aircraft_Type": data.Aircraft_Type,
        "Refund_Policy": data.Refund_Policy,
        "Departure_Time": data.Departure_Time,
        "Scrape_Time": data.Scrape_Time,
        "Departure_Location_Code": data.Departure_Location,
    }])

    return {"predicted_price": int(predict_airfare_real(input_df))}

# uvicorn src.api.main:app --reload

# curl -X 'POST' \
#   'http://127.0.0.1:8000/predict' \
#   -H 'Content-Type: application/json' \
#   -d '{
#   "Carry_on_Baggage": 7,
#   "Checked_Baggage": 20,
#   "Flight_Duration": 1.5,
#   "Fare_Class": "Economy",
#   "Airline": "Vietnam Airlines",
#   "Arrival_Location": "Hanoi",
#   "Aircraft_Type": "Airbus A321",
#   "Refund_Policy": ["Non-refundable"],
#   "Departure_Time": "2025-06-22 09:00:00",
#   "Scrape_Time": "2025-06-01 08:00:00",
#   "Departure_Location": "Ho Chi Minh City"
# }'