# Schema: Flight information data (`flight_prices_*.csv`)

| Column               | Data Type | Description |
|----------------------|-----------|-------------|
| Departure Location   | string    | Departure airport/city |
| Departure Time       | string    | Flight departure time (needs to be parsed to datetime) |
| Arrival Location     | string    | Arrival airport/city |
| Arrival Time         | string    | Flight arrival time (needs to be parsed to datetime) |
| Flight Duration      | string    | Duration of the flight |
| Aircraft Type        | string    | Type/model of the aircraft |
| Ticket Price         | string    | Full string containing airline, flight code, fare class |
| Passenger Type       | string    | Type of passenger (e.g., adult, child) |
| Number of Tickets    | int       | Number of tickets |
| Price per Ticket     | string    | E.g., '1,249,000 VNĐ' (needs cleaning to numeric) |
| Taxes & Fees         | string    | Tax and fee amount |
| Total Price          | string    | Final total price (needs cleaning to numeric) |
| Carry-on Baggage     | string    | Info about carry-on baggage (e.g., 7kg) |
| Checked Baggage      | string    | Info about checked baggage |
| Refund Policy        | string    | List of refund/change rules |
| Scrape Time          | string    | Time of data collection (needs parsing to datetime) |

### Notes:
- Ticket price columns should be converted to integers after removing 'VNĐ' and commas.
- Date and time columns should be standardized into datetime
- Ticket price can be split further into airline, flight code, fare class
