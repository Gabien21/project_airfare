import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime
import json
import os
from sqlalchemy import create_engine 
from dotenv import load_dotenv
load_dotenv()

def connect_to_db():
    server = os.getenv("DB_SERVER")
    database = os.getenv("DB_NAME")
    username = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    driver = 17
    conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+{driver}+for+SQL+Server"
    engine = create_engine(conn_str)
    return engine

def get_id_from_db(airline_selected): 
    db_engine = connect_to_db()
    query = f"SELECT * FROM INFO WHERE name = '{airline_selected}'"
    df =  pd.read_sql(query, db_engine)
    return df['airline_id'][0]

def get_airline_info(airline_id) :
    db_engine = connect_to_db()
    query = f"SELECT * FROM INFO WHERE airline_id = '{airline_id}'"
    return pd.read_sql(query, db_engine)

def get_airline_review(airline_id) :
    db_engine = connect_to_db()
    query = f"SELECT * FROM AIRLINE_REVIEW WHERE airline_id = '{airline_id}'"
    return pd.read_sql(query, db_engine)

def get_airline_mention(airline_id) :
    db_engine = connect_to_db()
    query = f"SELECT * FROM MENTION WHERE airline_id = '{airline_id}'"
    return pd.read_sql(query, db_engine)

def get_airline_rating(airline_id) :
    db_engine = connect_to_db()
    query = f"SELECT * FROM RATING WHERE airline_id = '{airline_id}'"
    return pd.read_sql(query, db_engine)

def get_airline_review_service(airline_id) :
    db_engine = connect_to_db()
    query = f"SELECT * FROM REVIEW_SERVICE WHERE airline_id = '{airline_id}'"
    return pd.read_sql(query, db_engine)


def get_airline_attribute(airline_id) :
    db_engine = connect_to_db()
    query = f"SELECT * FROM ATTRIBUTE WHERE airline_id = '{airline_id}'"
    return pd.read_sql(query, db_engine)

def display_star_rating(rating, max_stars=5):
    full_stars = int(rating)
    half_star = rating - full_stars >= 0.5
    empty_stars = max_stars - full_stars - int(half_star)

    stars = "★" * full_stars
    if half_star:
        stars += "⯪"  # or use "½", or a custom half-star emoji
    stars += "☆" * empty_stars

    return stars



# ========================== Streamlit UI ==========================
st.set_page_config(layout="wide")
st.title("✈️ Airline Information and Sentiment")

airline_selected = st.selectbox("Select Airline", ("VietJetAir", "Vietnam Airlines", "Bamboo Airways"))
if airline_selected: 
    col1, col2 = st.columns(2)

    with col1:
        st.header("Airline General Information")
        airline_id = get_id_from_db(airline_selected)

        airline_data = get_airline_info(airline_id)
        airline_review = get_airline_review(airline_id)
        airline_mention = get_airline_mention(airline_id)
        airline_rating = get_airline_rating(airline_id)
        airline_review_service = get_airline_review_service(airline_id)
        airline_attribute = get_airline_attribute(airline_id)

        st.write(f"Name: ", airline_data['name'][0])
        st.write(f"Phone: ", airline_data['phone'][0])
        st.write(f"Address: ", airline_data['address'][0])
        st.write(f"Website: ", airline_data['website'][0])
        st.write(f"Average Rating: ", airline_data['averating_rating'][0])
        st.write(f"Total Review: ", airline_data['total_review'][0])

    with col2:
        st.header("Overview Attribute")
        for _, row in airline_attribute.iterrows():
            st.write(f"{row['attribute_name']}: ", display_star_rating(row['rating']))
    


    st.header("Overview Rating")
    airline_rating['count'] = airline_rating['count'].replace({',': ''}, regex=True).astype(int)
    max_count = airline_rating['count'].max()
    for _, row in airline_rating.iterrows():
        percentage = row['count'] / max_count
        col1, col2, col3 = st.columns([0.3, 2, 0.5])
        with col1: st.write(row['rate_name'])
        with col2: st.progress(percentage)
        with col3: st.write(row['count'])

    st.header("Popular Mention")
    st.pills(label="",options=airline_mention['popular_mention'])

    st.header("Overall Review")
    data = airline_review['Sentiment'].value_counts()
    most_sentiment = data.sort_values(ascending=False).index[0]
    sentiment_count = data.sort_values(ascending=False).iloc[0]
    total_count = data.sort_values(ascending=False).sum()
    percent = round(sentiment_count/total_count *100, 2)
    if most_sentiment == 'Negative' :
        st.write(f"👎 {percent} % passenger wrote negative comments about this airline")
    elif most_sentiment == 'Positive' : 
        st.write(f"👍 {percent} % passenger wrote positive comments about this airline")
    else: 
        st.write(f"🤔 {percent} % passenger wrote neutral comments about this airline")


    st.header("Detail Review")
    col_left, col_right = st.columns([3, 1])  

    with col_right:
        st.markdown("### 🔍 Filters")
        
        sort_key = st.selectbox("Sort by Date", ("Ascending", "Descending"))

        options = ["Positive", "Neutral", "Negative"]
        selected = []

        for sentiment in options:
            if st.checkbox(sentiment, value=True):
                selected.append(sentiment)

    if sort_key == 'Ascending':
        airline_review.sort_values(by='Information', ascending=True, inplace=True)
    else:
        airline_review.sort_values(by='Information', ascending=False, inplace=True)

    if selected:
        filter_df = airline_review[airline_review["Sentiment"].isin(selected)]
    else:
        filter_df = airline_review

    with col_left:
        if filter_df.empty:
            st.info("No reviews available for the selected sentiment(s).")
        else:
            for i in range(5):
                review = filter_df.iloc[i]
                st.subheader(f"✈️ Review Title: {review['Title']}")
                st.write(f"Rating:",display_star_rating(review['Rating']))
                st.write(f"Sentiment: {'👍' if review['Sentiment'] == 'Positive' else '👎'} {review['Sentiment']}")
                st.write(f"Review Date: {review['Information']}")
                st.write(f"Full Review: {review['Full Review']}")

    
    