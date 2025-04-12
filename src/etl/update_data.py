import os
import logging
from datetime import datetime
from dateutil.relativedelta import relativedelta
from sqlalchemy import create_engine, text
from dotenv import load_dotenv
from src.utils.logger_utils import setup_logger

# ====================== Load environment variables ======================
load_dotenv()
username = os.getenv("DB_USER")
password = os.getenv("DB_PASSWORD")
server = os.getenv("DB_SERVER")
database = os.getenv("DB_NAME")
driver = 17  # Default ODBC driver



def delete_old_tickets_and_flights():
    """
    1. Delete old tickets where Scrape Time is over 3 months.
    2. Delete orphan flight schedules not referenced by any ticket.
    3. Remove duplicates in dimension tables: AIRPORT, AIRLINE, REFUND_POLICY.
    """
    try:
        conn_str = f"mssql+pyodbc://{username}:{password}@{server}/{database}?driver=ODBC+Driver+{driver}+for+SQL+Server"
        engine = create_engine(conn_str)

        current_date = datetime.now()
        cutoff_date = current_date - relativedelta(months=3)
        cutoff_str = cutoff_date.strftime('%Y-%m-%d %H:%M:%S')
        logging.info(f"Cutoff date for deletion: {cutoff_str}")

        with engine.begin() as conn:
            # Step 1: Delete old tickets
            delete_ticket_query = text("""
                DELETE FROM dbo.TICKET
                WHERE [Scrape Time] <= :cutoff_date
            """)
            ticket_result = conn.execute(delete_ticket_query, {"cutoff_date": cutoff_str})
            logging.info(f"Deleted {ticket_result.rowcount or 0} old ticket(s)")

            # Step 2: Delete orphan flight schedules
            delete_flight_query = text("""
                DELETE FROM dbo.FLIGHT_SCHEDULE
                WHERE NOT EXISTS (
                    SELECT 1 FROM dbo.TICKET t
                    WHERE 
                        t.[Flight Code] = FLIGHT_SCHEDULE.[Flight Code]
                        AND t.[Departure Time] = FLIGHT_SCHEDULE.[Departure Time]
                        AND t.[Departure Location Code] = FLIGHT_SCHEDULE.[Departure Location Code]
                )
            """)
            flight_result = conn.execute(delete_flight_query)
            logging.info(f"Deleted {flight_result.rowcount or 0} orphan flight schedule(s)")

            # Step 3: Drop duplicates in AIRPORT
            conn.execute(text("""
                WITH CTE AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY AirportCode, Location ORDER BY (SELECT NULL)) AS rn
                    FROM dbo.AIRPORT
                )
                DELETE FROM CTE WHERE rn > 1
            """))
            logging.info("Removed duplicates from AIRPORT")

            # Step 4: Drop duplicates in AIRLINE
            conn.execute(text("""
                WITH CTE AS (
                    SELECT *, ROW_NUMBER() OVER (PARTITION BY Airline_id, Airline ORDER BY (SELECT NULL)) AS rn
                    FROM dbo.AIRLINE
                )
                DELETE FROM CTE WHERE rn > 1
            """))
            logging.info("Removed duplicates from AIRLINE")

            # Step 5: Drop duplicates in REFUND_POLICY
            conn.execute(text("""
                WITH CTE AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY Airline_id, [Fare Class], [Refund Policy]
                        ORDER BY (SELECT NULL)
                    ) AS rn
                    FROM dbo.REFUND_POLICY
                )
                DELETE FROM CTE WHERE rn > 1
            """))
            logging.info("Removed duplicates from REFUND_POLICY")

        logging.info("Data cleanup completed successfully.")

    except Exception as e:
        logging.exception("Error during cleanup process")

if __name__ == "__main__":
    setup_logger(log_dir="logs")
    delete_old_tickets_and_flights()
