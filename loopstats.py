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
import pandas as pd

# Global variables
periods = [30, -30, 60, 90, 180, 360]
periods.sort()

debug = False
def convert_to_utc(time_str, timezone_str):
    """
    Convert a 24-hour time string and a timezone to UTC time.

    :param time_str: str, time in "HH:MM" format
    :param timezone_str: str, timezone (e.g., "America/New_York", "Asia/Tokyo")
    :return: str, UTC time in "HH:MM" format
    """
    # Parse the time string into a datetime object
    local_time = datetime.strptime(time_str, "%H:%M")

    # Localize the time to the given timezone
    local_tz = pytz.timezone(timezone_str)
    localized_time = local_tz.localize(local_time)

    # Convert to UTC
    utc_time = localized_time.astimezone(pytz.utc)

    # Format UTC time as "HH:MM"
    return utc_time.strftime("%H:%M")


def filter_by_time_np(data, start_time, end_time):
    """
    Filters a dataset using NumPy & Pandas for high-speed time filtering, including midnight crossing cases.

    :param data: list of dictionaries, each containing a "dateString" field in ISO format.
    :param start_time: str, start time in "HH:MM" format (24-hour).
    :param end_time: str, end time in "HH:MM" format (24-hour).
    :return: filtered list of dictionaries.
    """
    # Convert JSON data to DataFrame
    df = pd.DataFrame(data)

    # Convert 'dateString' to pandas datetime
    df["time"] = pd.to_datetime(df["dateString"]).dt.time

    # Convert start & end time to datetime.time objects
    start_time = pd.to_datetime(start_time, format="%H:%M").time()
    end_time = pd.to_datetime(end_time, format="%H:%M").time()

    # Fast filtering using NumPy
    if start_time <= end_time:
        mask = (df["time"] >= start_time) & (df["time"] <= end_time)
    else:
        mask = (df["time"] >= start_time) | (df["time"] <= end_time)

    return df[mask].drop(columns=["time"]).to_dict(orient="records")


def process_stats(row, startdate, enddate, ptNSCol, days, base_columns, starttime, endtime):
    """Process A1c data and return relevant statistics or empty values if no data."""
    # Retrieve data
    data, response_url = dataretrieve(row[ptNSCol], startdate, enddate)
    if not data:
        if debug:
            print("no data on nightscout!")
        return ("",) * len(base_columns), []

    # Filter by time only if valid starttime and endtime are provided
    if starttime and endtime and starttime.strip() and endtime.strip():
        try:
            tz = timezone(row[ptNSCol], enddate)
            starttimeutc = convert_to_utc(starttime, tz)
            endtimeutc = convert_to_utc(endtime, tz)
            data = filter_by_time_np(data, starttimeutc, endtimeutc)
        except Exception:
            if debug:
                print("error with time")
            return ("",) * len(base_columns), []

    # Return processed stats
    return (startdate, enddate, days) + GMIstats(data, days), data

def adddays(startdate, days = 90):
    enddate = (datetime.fromisoformat(startdate) + timedelta(days)).isoformat().split("T")[0]
    return enddate

def process_row(row, ptNSStatus, ptLOOPStart, ptIDCol, ptLinkCol, ptNSCol, base_columns, ptHardware, starttime, endtime):
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
                result, data = process_stats(row, startdate, enddate, ptNSCol, days, base_columns, starttime, endtime)
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


def loopstats(snap , name="loop", starttime = "", endtime = ""): # enter a time
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

        if debug:
            # Single-process debugging
            results = []
            for row in tqdm(rows, total=len(rows), desc="Processing Patients"):
                print(row)
                try:
                    result = process_row(
                        row, ptNSStatus, ptLOOPStart, ptIDCol, ptLinkCol, ptNSCol,
                        base_columns, ptHardware, starttime, endtime
                    )
                    print(f'\nResults: {result}')
                    if result:
                        results.append(result)
                except Exception as e:
                    print(f"Error processing row: {e}")
        else:
            # Original parallel code
            manager = Manager()
            results = manager.list()

            with ProcessPoolExecutor() as executor:
                futures = [
                    executor.submit(
                        process_row, row, ptNSStatus, ptLOOPStart, ptIDCol, ptLinkCol,
                        ptNSCol, base_columns, ptHardware, starttime, endtime
                    )
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
        with open(f"gitignore/results_{str(name)}_{starttime}-{endtime}.csv", 'w', newline='', buffering=1) as f:
            writer = csv.writer(f)
            writer.writerow(final_headers)  # Write header
            writer.writerows(results)  # Write all results at once

        print("Results exported.")