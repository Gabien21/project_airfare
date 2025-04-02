# Schema: Airline Customer Reviews (`*_all_reviews_data.csv`)
| Column    | Data Type | Description|
|-----------|-----------|------------|
| Rating | string | E.g., '1.0 of 5 bubbles' (need convert to float) |
| Title | string | Review title |
| Full Review | string | Full context of the customer review |
| Information | string | Flight information (e.g., travel date, route) |
| Service Ratings | string (JSON) | JSON list of the service ratings (needs parsing) |

### Notes:
- `Rating` should be extracted as numeric (1.0 to 5.0).
- `Service Ratings` contains JSON-like structure → use `json.loads` or `eval` to parse.
- Travel date can be extracted from `Information`.