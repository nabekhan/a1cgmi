"""
Time below range (<3 mmol/L)
Time below range (3–3.8 mmol/L)
Time in range (3.9–10 mmol/L)
Time above range (10.1–13.8 mmol/L)
Time above range (>13.9 mmol/L)
Time in fluctuation
Time in rapid fluctuation
Average glucose
Standard deviation
Glycemic Management Indicator (GMI)
"""

"""
1. Retrieve data between set date + days
2. Calculate Stats
3. Output to CSV file
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

# Global variables
periods = [-30, 30, 60, 90, 180, 360]

def process_stats(row, startdate, enddate, ptNSCol, days, base_columns):
    """Process A1c data and return relevant statistics or empty values if no data."""
    data, response_url = dataretrieve(row[ptNSCol], startdate, enddate)
    if data:
        return GMIstats(data, days), data
    # Return empty strings and empty list for missing data
    return ("",) * len(base_columns), []

def loopperiod(startdate, days = 90):
    enddate = (datetime.fromisoformat(startdate) + timedelta(days)).isoformat().split("T")[0]
    return enddate

def process_row(row, ptNSStatus, ptLOOPStart, ptIDCol, ptLinkCol, ptNSCol, base_columns, ptHardware):
    results = []
    loopstart = row[ptLOOPStart]
    try:
        ns_deployed = int(row[ptNSStatus])
    except:
        ns_deployed = 0
    if ns_deployed == 1:
        if loopstart:
            loopperiods = []
            for period in periods:
                if period < 0:
                    loopperiods.append((loopperiod(loopstart, period), loopstart, abs(period)))
                else:
                    loopperiods.append((loopstart, loopperiod(loopstart, period), abs(period)))
            for startdate, enddate, days in loopperiods:
                result, data = process_stats(row, startdate, enddate, ptNSCol, days, base_columns)
                results.extend(result)

            if any(results[i] for i in range(0, len(results), len(base_columns))):
                return [
                    int(float(row[ptIDCol].replace(',', ''))),
                    row[ptLinkCol],
                    row[ptHardware],
                    loopstart,
                    *results
                ]
    return []  # Return empty lists instead of None


def daily_avg_blood_sugar(daily_data, ptID):
    """Calculate daily average blood sugar and export to CSV"""
    daily_sgv = defaultdict(list)
    for entry in daily_data:
        if 'sgv' in entry:
            date = datetime.fromtimestamp(entry['date'] / 1000).strftime('%Y-%m-%d')
            daily_sgv[date].append(entry['sgv'])

    daily_avg_results = [(ptID, date, sum(sgv_list) / len(sgv_list), len(sgv_list)) for date, sgv_list in daily_sgv.items()]
    return daily_avg_results


def loopstats(name="loop"):
    snap = 'gitignore/2024-10-30withNSs.csv'

    with open(snap, mode="r") as snapdata:
        readfile = csv.reader(snapdata)
        headers = next(readfile)

        # Define column indices
        ptIDCol = headers.index('DPD_ID')
        ptLinkCol = headers.index('link')
        ptNSCol = headers.index('ns_uuid')
        ptNSStatus = headers.index('ns_status')
        ptLOOPStart = headers.index('LoopingStartDate')
        ptHardware = headers.index('Hardware')

        base_columns = ["cgm", "count", "percentdata", "avgglucose", "STD", "ptGMI", "TBR", "TIR (3.9–10 mmol/L)",
                        "TAR","verylow (<3 mmol/L)", "low (3–3.9 mmol/L)", "high (10–13.9 mmol/L)", "veryhigh (>13.8 mmol/L)",
                        "timefluc", "timerapid"]

        final_headers = ["ID", "link", "loopstart", "ptHardware"] + [f"{col} ({str(period)})" for period in periods for col in base_columns]

        # Read all rows into a list to count them for progress bar
        rows = list(readfile)

        # Set up shared lists
        manager = Manager()
        results = manager.list()

        # Updated ProcessPoolExecutor with shared list for daily data
        with ProcessPoolExecutor() as executor:
            futures = [
                executor.submit(process_row, row, ptNSStatus, ptLOOPStart, ptIDCol, ptLinkCol, ptNSCol, base_columns, ptHardware)
                for row in rows
            ]

            for future in tqdm(futures, total=len(rows), desc="Processing Patients"):
                try:
                    result = future.result()
                    if result:
                        results.append(result)
                except Exception as e:
                    print(f"Error processing row: {e}")

        # Write main results to CSV
        with open(f"gitignore/results_{str(name)}.csv", 'w', newline='', buffering=1) as f:
            writer = csv.writer(f)
            writer.writerow(final_headers)  # Write header
            writer.writerows(results)  # Write all results at once

        print("Results exported.")