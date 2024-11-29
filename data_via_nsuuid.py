"""
This module takes NS_UUIDs, a start and end date, and retrieves their data
"""
import requests

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

# Obtain data
def dataretrieve(ns_uuid, startDate, endDate):
    if ns_uuid:
        url = jsonurl(ns_uuid, startDate, endDate)
        auth = ('_cgm', 'queries_')  # Authentication credentials
        response = requests.get(url, auth=auth)
        # Print the final URL being used for the request
        #print("Request URL:", response.url)
        response.raise_for_status()  # Check if the request was successful
        data = response.json()
        data = sorted(data, key=lambda d: d['date']) # sort data from first to last date
        return data, response.url
    else:
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