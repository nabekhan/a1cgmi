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
from collections import defaultdict
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm  # For progress bar
from multiprocessing import Manager
from sugarstats import *

def startA1cdate(endA1cdate, days = 90):
    startA1cdate = (datetime.fromisoformat(endA1cdate) - timedelta(days)).isoformat().split("T")[0]
    return startA1cdate

def A1cdata(ns_uuid, A1cDate, days = 90):
    data, responseurl = dataretrieve(ns_uuid, startA1cdate(A1cDate, days), A1cDate)
    return data, responseurl

def process_A1c(row, ptA1cDate, ptA1c, ptNSCol, days, base_columns):
    """Process A1c data and return relevant statistics or empty values if no data."""
    A1c_date = row[ptA1cDate]  # Access by index for lists
    if A1c_date:
        data, response_url = A1cdata(row[ptNSCol], A1c_date, days)
        if data:
            A1c_value = row[ptA1c]  # Access by index for lists
            return GMIstats(data, days) + (A1c_value, A1c_date), data
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


def a1cgmi(days=90):
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

        base_columns = ["cgm", "count", "percentdata", "avgglucose", "STD", "ptGMI", "TBR", "TIR (3.9–10 mmol/L)",
                        "TAR","verylow (<3 mmol/L)", "low (3–3.9 mmol/L)", "high (10–13.9 mmol/L)", "veryhigh (>13.8 mmol/L)",
                        "timefluc", "timerapid", "A1c_value", "A1c_date"]
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