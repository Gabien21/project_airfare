import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
from src.deployment.inference import predict_airfare_real

# ========================== Setup Path Constants ==========================
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../"))
DATA_DIR = os.path.join(BASE_DIR, "data/clean/flight_prices")

# ========================== Streamlit UI ==========================
st.set_page_config(layout="wide")
st.title("✈️ Airfare Price Prediction")


with open(os.path.join(DATA_DIR, "options.json"), "r", encoding="utf-8") as f:
    options = json.load(f)

# -------- User Input Form --------
col1, col2 = st.columns(2)
with col1:
    st.subheader("Input Selection")
    c1, c2 = st.columns(2)
    with c1:
        departure = st.selectbox("Departure", options['Departure Location'])
        departure_date = st.date_input("Departure Date")
        airline = st.selectbox("Airline", options['Airline'])
    with c2:
        arrival = st.selectbox("Arrival", options['Arrival Location'])
        departure_time = st.time_input("Departure Time")
        duration = st.slider("Duration (Minutes)",
                             int(np.round(options['Flight Duration']['min'][arrival]*60)),
                             int(np.round(options['Flight Duration']['max'][arrival]*60)),
                             step=5)

    c1, c2 = st.columns(2)
    with c1:
        fare_class = st.selectbox("Fare Class", options['Baggage'][airline].keys())
        carry_baggage = st.selectbox("Carry-on Baggage (kg)", options['Baggage'][airline][fare_class]['carry_on'])
    with c2:
        aircraft_type = st.selectbox("Aircraft Type", options['Aircraft Type'])
        checked_baggage = st.selectbox("Checked Baggage (kg)", options['Baggage'][airline][fare_class]['checked'])

    refund_policy = st.multiselect("Refund Policy", options['Refund Policy'][airline][fare_class])
    departure_datetime = datetime.combine(departure_date, departure_time)

# -------- Summary Column --------
with col2:
    st.subheader("Selected Input Summary")
    st.markdown(f"**Departure:** {departure}")
    st.markdown(f"**Arrival:** {arrival}")
    st.markdown(f"**Airline:** {airline}")
    st.markdown(f"**Duration:** {duration} minutes")
    st.markdown(f"**Carry-on Baggage:** {carry_baggage} kg")
    st.markdown(f"**Checked Baggage:** {checked_baggage} kg")
    st.markdown(f"**Fare Class:** {fare_class}")
    st.markdown(f"**Aircraft Type:** {aircraft_type}")
    st.markdown(f"**Refund Policy:** {refund_policy}")
    st.markdown(f"**Departure datetime:** `{departure_datetime.strftime('%Y-%m-%d %H:%M')}`")
    st.markdown(f"**Today:** `{datetime.now().strftime('%Y-%m-%d %H:%M')}`")

# -------- Prediction Button --------
if st.button("Predict Price"):
    input_df = pd.DataFrame({
        "Carry-on_Baggage": [carry_baggage],
        "Checked_Baggage": [checked_baggage],
        "Flight_Duration": [np.round(duration/60, 2)],
        "Fare_Class": [fare_class],
        "Airline_id": [airline],
        "Arrival_Location_Code": [arrival],
        "Aircraft_Type": [aircraft_type],
        "Refund_Policy": [refund_policy],
        "Departure_Time": [departure_datetime.strftime('%Y-%m-%d %H:%M')],
        "Scrape_Time": [datetime.now().strftime('%Y-%m-%d %H:%M')],
        "Departure_Location_Code": [departure],
    })

    # -------- Show Prediction Result --------
    st.subheader("Predicted Price")
    pred_price = int(predict_airfare_real(input_df))
    st.success(f"{int(pred_price):,} VND")