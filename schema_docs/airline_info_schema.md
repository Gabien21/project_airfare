# Schema: Airline General Info (`*_general_info.txt`)

### Raw data is in key-value format, one item per line, including:

- Name
- Phone
- Address 
- Website
- Average Rating
- Total review
- Popular mention: [list] 
- Attributes: {dict} 
- Total rating: {dict}

### Notes:
- Lines can be parsed by splitting with `:`, being cautious with nested dictionaries.
- `Attributes` and `Total rating` are in Python dict format → use `eval()` or `json.loads()` to convert.
- `Popular mention` is a list of frequent keywords/phrases from reviews.