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
from concurrent.futures import ProcessPoolExecutor
from tqdm import tqdm  # For progress bar
from multiprocessing import Manager
from sugarstats import *

# Global variables
periods = [30, -30, 60, 90, 180, 360]
periods.sort()

def process_stats(row, startdate, enddate, ptNSCol, days, base_columns):
    """Process A1c data and return relevant statistics or empty values if no data."""
    data, response_url = dataretrieve(row[ptNSCol], startdate, enddate)
    if data:
        return (startdate, enddate, days, ) + GMIstats(data, days), data
    # Return empty strings and empty list for missing data
    return ("",) * len(base_columns), []

def adddays(startdate, days = 90):
    enddate = (datetime.fromisoformat(startdate) + timedelta(days)).isoformat().split("T")[0]
    return enddate

def process_row(row, ptNSStatus, ptLOOPStart, ptIDCol, ptLinkCol, ptNSCol, base_columns, ptHardware):
    results = []
    loopstart = row[ptLOOPStart]
    try:
        ns_deployed = int(float(row[ptNSStatus]))
    except:
        ns_deployed = 0
    if ns_deployed == 1:
        if loopstart:
            loopperiods = []
            first_positive_found = False
            for i, period in enumerate(periods):
                if period < 0:
                    loopperiods.append((adddays(loopstart, period), adddays(loopstart, -1), abs(period)))
                elif (period > 0) & (not first_positive_found):
                    end_date = adddays(loopstart, period)
                    loopperiods.append((loopstart, end_date, abs(period)))
                    next_start_date = adddays(end_date, 1)
                    first_positive_found = True
                else:
                    end_date = adddays(loopstart, period)
                    loopperiods.append((next_start_date, end_date, abs(period)))
                    next_start_date = adddays(end_date, 1)

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


def loopstats(snap , name="loop"):
    snap

    with open(snap, mode="r") as snapdata:
        readfile = csv.reader(snapdata)
        headers = next(readfile)

        # Define column indices
        ptIDCol = headers.index('key')
        ptLinkCol = headers.index('link')
        ptNSCol = headers.index('ns_uuid')
        ptNSStatus = headers.index('ns_status')
        ptLOOPStart = headers.index('OSAID startdate')
        ptHardware = headers.index('Software')

        base_columns = ["startdate", "enddate", "days", "cgm", "count", "percentdata", "avgglucose", "STD", "ptGMI", "TBR", "TIR (3.9–10 mmol/L)",
                        "TAR","verylow (<3 mmol/L)", "low (3–3.9 mmol/L)", "high (10–13.9 mmol/L)", "veryhigh (>13.8 mmol/L)",
                        "timefluc", "timerapid"]

        final_headers = ["ID", "link", "ptHardware", "loopstart"] + [f"{col} ({str(period)})" for period in periods for col in base_columns]

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