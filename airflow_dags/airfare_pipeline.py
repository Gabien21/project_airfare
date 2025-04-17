from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import sys
import os
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

default_args = {
    "owner": "gabien",
    "retries": 2,
    "retry_delay": timedelta(minutes=1),
}

with DAG(
    dag_id = "airfare_pipeline",
    default_args=default_args,
    description="DAG process data and train a model to predict airfare",
    schedule="@daily",
    start_date=datetime(2025, 4, 15),
    catchup=False,
    max_active_tasks=5,
) as dag:

    def crawl_data_SGN_to_DAD():
        from src.crawler.abay_form_oneway import craw_pipeline, choose_datetime
        start_date_str, end_date_str = choose_datetime(num_month=1)
        craw_pipeline(
            departure="SGN",
            destination="DAD",
            date_str=start_date_str,
            end_date_str=end_date_str,
            save_dir="data/raw"
        )

    def crawl_data_SGN_to_HAN():
        from src.crawler.abay_form_oneway import craw_pipeline, choose_datetime
        start_date_str, end_date_str = choose_datetime(num_month=1)
        craw_pipeline(
            departure="SGN",
            destination="HAN",
            date_str=start_date_str,
            end_date_str=end_date_str,
            save_dir="data/raw"
        )

    def preprocess_flight_data():
        from src.etl.preprocessing_flight_prices import ETL
        # ETL(data_dir=datetime.now().strftime("%d_%m_%Y"))
        ETL(data_dir="aaa")

    def update_and_clean_data():
        from src.etl.update_data import delete_old_tickets_and_flights
        delete_old_tickets_and_flights()

    def preprocess_to_train_model():
        from src.modeling.preprocess_data_for_modeling import preprocess_for_modeling
        preprocess_for_modeling()

    def train_model():
        from src.modeling.modeling_data import model_data
        model_data()

    def test_infer():
        from src.deployment.inference import predict_airfare_real
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

task_crawl_dad = PythonOperator(task_id="crawl_sgn_dad", python_callable=crawl_data_SGN_to_DAD, dag=dag)
task_crawl_han = PythonOperator(task_id="crawl_sgn_han", python_callable=crawl_data_SGN_to_HAN, dag=dag)
task_preprocess = PythonOperator(task_id="preprocess_data", python_callable=preprocess_flight_data, dag=dag)
task_update_clean = PythonOperator(task_id="update_clean_data", python_callable=update_and_clean_data, dag=dag)
task_preprocess_model = PythonOperator(task_id="prepare_training_data", python_callable=preprocess_to_train_model, dag=dag)
task_train = PythonOperator(task_id="train_model", python_callable=train_model, dag=dag)
task_test = PythonOperator(task_id="test_infer", python_callable=test_infer, dag=dag)

[task_crawl_dad, task_crawl_han] >> task_preprocess >> task_update_clean >> task_preprocess_model >> task_train >> task_test