"""
This module takes NS_UUIDs, a start and end date, and retrieves their data
"""
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import pytz
from datetime import datetime

# Create URL from ns_uuid
def jsonurl(ns_uuid, startDate, endDate):
    base_url = f"https://_cgm:queries_@{ns_uuid}.cgm.bcdiabetes.ca/get-glucose-data"
    params = {
        "gte": f"{startDate}Z",
        "lte": f"{endDate}Z"
    }
    query_string = f"gte={params['gte']}&lte={params['lte']}"
    full_url = f"{base_url}?{query_string}"
    return full_url

# Finder
def find_timezone(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if key == "timezone":
                return value
            result = find_timezone(value)
            if result:
                return result
    elif isinstance(data, list):
        for item in data:
            result = find_timezone(item)
            if result:
                return result
    return None  # Return None if not found

# Get timezone - Broken
def timezone(ns_uuid, endDate):
    # Define a session with increased retry settings
    session = requests.Session()
    retries = Retry(
        total=3,  # Number of total retries
        backoff_factor=1,  # Wait time increases exponentially: 1s, 2s, 4s, etc.
        allowed_methods={"GET", "POST"}  # Retry only for GET and POST requests
    )
    # Mount the retry strategy to HTTPS connections
    session.mount("https://", HTTPAdapter(max_retries=retries))

    #profileurl = f"https://{ns_uuid}.cgm.bcdiabetes.ca/api/v1/profiles?find[startDate][$gte]={startDate}&count=10000000" #ex date 2025-01-08
    profileurl = f"https://{ns_uuid}.cgm.bcdiabetes.ca/api/v1/profile.json?find[startDate][$lte]={endDate}" #ex date 2025-01-08
    jsonurl(ns_uuid, "", "")
    response = session.get(profileurl, timeout=10)
    response.raise_for_status()
    tz = find_timezone(response.json()[0])
    return tz

# Obtain data
def dataretrieve(ns_uuid, startDate, endDate, max_retries=10):
    if not ns_uuid:
        return "", ""

    delay = 3  # initial delay in seconds
    for attempt in range(max_retries):
        try:
            url = jsonurl(ns_uuid, startDate, endDate)
            auth = ('_cgm', 'queries_')  # Authentication credentials
            response = requests.get(url, auth=auth, timeout=(20, 60))
            response.raise_for_status()  # Check if the request was successful
            data = response.json()
            data = sorted(data, key=lambda d: d['date'])  # sort data from first to last date
            return data, response.url
        except requests.exceptions.RequestException as e:
            if attempt < max_retries - 1:
                print(f"Attempt {attempt + 1} on {url} failed: {e}. Retrying in {delay} seconds...")
                time.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                print(f"Attempt {attempt + 1} on {url} failed: {e}. No more retries.")
                return "", ""

# Subset sugars from data
def sugarreadings(data):
    # Select all glucose readings
    sgv_values = list([entry['sgv'] for entry in data if 'sgv' in entry])
    # Select dates of glucose readings
    sgv_dates = list([entry['date'] for entry in data if 'sgv' in entry])
    # Combine glucose, date readings into list
    sgv_valuesdt = []
    for index, value in enumerate(sgv_values):
            sgv_valuesdt.append([value, sgv_dates[index]])

    return sgv_values, sgv_valuesdt