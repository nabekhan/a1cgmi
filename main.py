"""
This script is designed to determine the GMI for a selected period
It should:
1) retrieve data
2) retrive a1c + dates from snapshot
2) calculate the average blood sugar and TIR, TBR, TAR
3) export results
"""
import warnings
warnings.filterwarnings("ignore")
from datetime import datetime, timedelta
import csv
import requests
import numpy as np
from collections import defaultdict
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
import pandas as pd
from tqdm import tqdm  # For progress bar
from multiprocessing import Manager

def jsonurl(ns_uuid, startDate, endDate):
    base_url = f"https://_cgm:queries_@{ns_uuid}.cgm.bcdiabetes.ca/get-glucose-data"
    params = {
        "gte": f"{startDate}Z",
        "lte": f"{endDate}Z"
    }
    query_string = f"gte={params['gte']}&lte={params['lte']}"
    full_url = f"{base_url}?{query_string}"
    return full_url

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

def startA1cdate(endA1cdate, days = 90):
    startA1cdate = (datetime.fromisoformat(endA1cdate) - timedelta(days)).isoformat().split("T")[0]
    return startA1cdate

def A1cdata(ns_uuid, A1cDate, days = 90):
    data, responseurl = dataretrieve(ns_uuid, startA1cdate(A1cDate, days), A1cDate)
    return data, responseurl

def cgmtype(device):
    if "lvconnect" in device:
        return "libre"
    else:
        return "dexcom"
def dataPercent(glucoselist, type, days=90):
    if type == "libre":
        percent = len(glucoselist)/(96*days) * 100
    else:
        percent = len(glucoselist)/(288*days) * 100
    return percent

def average(lst):
    return sum(lst) / len(lst)

def GMI(glucoseavg):
    GMI = 3.31 + 0.02392 * glucoseavg
    return(GMI)

def compareA1c(A1c, GMI):
    dif = GMI - A1c
    return dif

def timeinfluc(valdtlist, rapid = False):
    events = 0
    count = 0
    if rapid:
        t1 = 11
    else:
        t1 = 6
    for index, entry in enumerate(valdtlist[1:], 1):
        cur_entry = entry[0]
        cur_date = entry[1]
        prev_entry = valdtlist[index-1][0]
        prev_date = valdtlist[index-1][1]
        if index == 0:
            continue
        elif (cur_date - prev_date > (6 * 60 * 1000)): #skip max gap
            continue
        elif cur_date - prev_date>0:
            events +=1
            delta = abs(cur_entry - prev_entry)
            timedelta = cur_date - prev_date
            fluc = delta/timedelta
            if fluc >= (t1 / (1000 * 60 * 5)):
                count +=1
    return(count/events * 100)

def GMIstats(data, A1c, days = 90):
    try:
        retrievedevice = data[0]['device']
    except:
        retrievedevice = ""
    cgm = cgmtype(retrievedevice)

    # Get percent data based on brand
    percentdata = dataPercent(data, cgm, days)

    # Get readings and calculate GMI
    sgv_values = list([entry['sgv'] for entry in data if 'sgv' in entry])
    sgv_dates = list([entry['date'] for entry in data if 'sgv' in entry])
    sgv_valdt = []
    for index, value in enumerate(sgv_values):
            sgv_valdt.append([value, sgv_dates[index]])
    timefluc = timeinfluc(sgv_valdt)
    timerapid = timeinfluc(sgv_valdt, True)
    count = len(sgv_values)
    avgglucose = average(sgv_values)
    std = np.std(sgv_values)
    ptGMI = GMI(avgglucose)
    A1cdif = compareA1c(float(A1c), ptGMI)
    TBR = (sum(i < 70 for i in sgv_values))/count*100
    TAR = (sum(i > 180 for i in sgv_values))/count*100
    TIR = 100 - TAR - TBR
    verylow = (sum(i < 54 for i in sgv_values))/count*100
    low = (sum(54 <= i < 70 for i in sgv_values)) / count * 100
    high = (sum(180 < i <= 248.615 for i in sgv_values)) / count * 100
    veryhigh = (sum(i > 248.615 for i in sgv_values))/count*100
    return (cgm, count, percentdata, avgglucose, std, ptGMI, A1cdif, TBR, TIR, TAR,
            timefluc, timerapid, verylow, low, high, veryhigh)


def process_A1c(row, ptA1cDate, ptA1c, ptNSCol, days, base_columns):
    """Process A1c data and return relevant statistics or empty values if no data."""
    A1c_date = row[ptA1cDate]  # Access by index for lists
    if A1c_date:
        data, response_url = A1cdata(row[ptNSCol], A1c_date, days)
        if data:
            A1c_value = row[ptA1c]  # Access by index for lists
            return GMIstats(data, A1c_value, days) + (A1c_value, A1c_date), data
    # Return empty strings and empty list for missing data
    return ("",) * len(base_columns), []


def process_row(row, a1c_mappings, ptIDCol, ptLinkCol, ptNSCol, days, base_columns):
    results = []
    daily_data = []
    for ptA1cDate, ptA1c in a1c_mappings:
        result, data = process_A1c(row, ptA1cDate, ptA1c, ptNSCol, days, base_columns)
        dailies = daily_avg_blood_sugar(data, row[ptIDCol])
        results.extend(result)
        daily_data.extend(dailies)

    if any(results[i] for i in range(0, len(results), len(base_columns))):
        return [
            int(float(row[ptIDCol].replace(',', ''))),
            row[ptLinkCol],
            *results
        ], daily_data
    return [], []  # Return empty lists instead of None


def daily_avg_blood_sugar(daily_data, ptID):
    """Calculate daily average blood sugar and export to CSV"""
    daily_sgv = defaultdict(list)
    for entry in daily_data:
        if 'sgv' in entry:
            date = datetime.fromtimestamp(entry['date'] / 1000).strftime('%Y-%m-%d')
            daily_sgv[date].append(entry['sgv'])

    daily_avg_results = [(ptID, date, sum(sgv_list) / len(sgv_list), len(sgv_list)) for date, sgv_list in daily_sgv.items()]
    return daily_avg_results


def main(days=90):
    snap = 'gitignore/DPD 2024-10-30.csv'

    with open(snap, mode="r") as snapdata:
        readfile = csv.reader(snapdata)
        headers = next(readfile)

        # Define column indices
        ptIDCol = headers.index('DPD_ID')
        ptLinkCol = headers.index('link')
        ptNSCol = headers.index('ns_uuid')
        ptA1c1 = headers.index('A1c')
        ptA1c1date = headers.index('A1c_datetime')
        ptA1c2 = headers.index('A1c_previous')
        ptA1c2date = headers.index('A1c_previous_datetime')
        ptA1c3 = headers.index('A1c_3d_most_recent')
        ptA1c3date = headers.index('A1c_3d_most_recent_datetime')

        a1c_mappings = [
            (ptA1c1date, ptA1c1),
            (ptA1c2date, ptA1c2),
            (ptA1c3date, ptA1c3)
        ]

        base_columns = ["cgm", "count", "percentdata", "avgglucose", "STD", "ptGMI", "A1cdif", "TBR", "TIR", "TAR",
                        "timefluc", "timerapid", "verylow", "low", "high", "veryhigh", "A1c_value", "A1c_date"]
        final_headers = ["ID", "link"] + [f"{col}{i}" for i in range(1, 4) for col in base_columns]

        # Read all rows into a list to count them for progress bar
        rows = list(readfile)

        # Set up shared lists
        manager = Manager()
        results = manager.list()
        all_daily_data = manager.list()

        # Updated ProcessPoolExecutor with shared list for daily data
        with ProcessPoolExecutor() as executor:
            futures = [
                executor.submit(process_row, row, a1c_mappings, ptIDCol, ptLinkCol, ptNSCol, days, base_columns)
                for row in rows
            ]

            for future in tqdm(futures, total=len(rows), desc="Processing Patients"):
                try:
                    result, daily_data = future.result()
                    if result:
                        results.append(result)
                        all_daily_data.extend(daily_data)
                except Exception as e:
                    print(f"Error processing row: {e}")

        # Write main results to CSV
        with open(f"gitignore/results_{str(days)}.csv", 'w', newline='', buffering=1) as f:
            writer = csv.writer(f)
            writer.writerow(final_headers)  # Write header
            writer.writerows(results)  # Write all results at once

        print("Results exported.")

        with open(f"gitignore/daily_avg_blood_sugar_{str(days)}.csv", 'w', newline='', buffering=1) as f:
            writer = csv.writer(f)
            writer.writerow(["ID", "Date", "Average Blood Sugar", "Data Points"])  # Write header
            writer.writerows(all_daily_data)

        print("Daily results exported.")


if __name__ == "__main__":
    main(90)