import csv
from pytrends.request import TrendReq
import pandas as pd
import datetime

pytrends = TrendReq(hl='en-US', tz=360)

### Part 1. Read Operation (Extract)
def fetch_google_trends_data(keywords):
    """
    Fetches google trends containing specific keywords.
    """
    pytrends.build_payload(keywords, cat=0, timeframe="today 12-m", geo="", gprop="")

    # Get interest over time
    data = pytrends.interest_over_time()
    data.to_csv('raw/pytrends.csv', index = True)
    return data
    

### Part 2. Write Operation (Load)
def save_to_csv(data, filename):
    """Saves weather data to a CSV file."""
    print(data, len(data))
    if 'isPartial' in data.columns:
        print("Dropping")
        data.drop(columns = ['isPartial'], inplace = True)
    
    data.to_csv(filename, index = True)
    return None

if __name__ == "__main__":
    print("True")
    keywords = ["remote work", "work from home", "offline work", "digital work", "flexible work"]
    posts = fetch_google_trends_data(keywords)
    print(posts)
    save_to_csv(posts, "./datasets/pytrends.csv")
    print("Reddit data saved to pytrends.csv")
        
