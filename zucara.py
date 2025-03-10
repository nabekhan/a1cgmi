from datetime import time
from dateutil import parser
import urllib.request
from main import *
import concurrent.futures

def average(lst):
    return sum(lst) / len(lst)


def glucosedata(cgmdata):
    glucoselist = []
    for row in cgmdata:
        glucoselist.append(int(row[2]))
    return glucoselist

def GMI(glucoseavg):
    GMI = 3.31 + 0.02392 * glucoseavg
    return(GMI)

def GMI2008(glucoseavg):
    GMI = (glucoseavg + 43.9)/28.3
    return(GMI)
def cgmtype(device):
    if "lvconnect" in device:
        return "libre"
    else:
        return "dexcom"

def dataPercent(glucoselist, type):
    if type == "libre":
        percent = len(glucoselist)/720 * 100
    else:
        percent = len(glucoselist)/2159 * 100
    return percent

def compareA1c(A1c, GMI):
    dif = GMI - A1c
    return dif

def startDateCalc(enddate):
    enddate = datetime.strptime(enddate, "%Y-%m-%d")
    startdate = enddate - timedelta(days=30) # 1 month of days
    return startdate.strftime('%Y-%m-%d')

def findcolumn(list, substring):
    i = -1
    for col in list[0]:
        i = i + 1
        if substring in col:
            return i
    return "Not Found"

def writeresults(filename, data):
    import csv
    fieldnames = ["ID", "AvgGlucose", "GMI", "Percent",
                  "Type", "TBRCount", "Data List", "NSURL"]
    with open(filename, mode="w", newline="") as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        writer.writeheader()
        for row in data:
            writer.writerow(row)


def tbrcalc(data, threshold, brand):
    totalcount = 0
    datecount = {}
    all_values = []
    for key, item in data.items():
        # Sort and deduplex
        data_sorted = sorted(item, key=lambda x: x["dateString"])
        deduped = []
        last_timestamp = None
        for entry in data_sorted:
            # Convert dateString to a NumPy datetime64 (millisecond precision)
            current_ts = np.datetime64(entry["dateString"], "ms")

            if last_timestamp is None:
                # First entry: always keep
                deduped.append(entry)
                last_timestamp = current_ts
            else:
                # Compute difference from the last kept entry
                diff = current_ts - last_timestamp
                # Keep only if ≥ 3 minutes later
                if diff >= np.timedelta64(3, "m"):
                    deduped.append(entry)
                    last_timestamp = current_ts

        #print(deduped)
        values = np.array([d["sgv"] for d in deduped if "sgv" in d])
        all_values += values.tolist()

        # brand setup
        if brand == "libre":
            runlength = 2
        else:
            runlength = 3

        # Boolean array: True where values < threshold
        below_threshold = values < threshold
        count = 0
        current_run_length = 0

        for b in below_threshold:
            if b:
                # Still below threshold, continue the run
                current_run_length += 1
            else:
                # We've hit a value above threshold, check if the run was >= 3
                if current_run_length >= runlength:
                    count += 1
                # Reset run length
                current_run_length = 0

        # If we ended on a run, check that as well
        if current_run_length >= runlength:
            count += 1

        totalcount += count
        datecount[key] = count

    print(datecount.items())
    return totalcount, all_values, datecount


def isNowInTimePeriod(startTime, endTime, nowTime):
    if startTime < endTime:
        return nowTime >= startTime and nowTime <= endTime
    else:
        # Over midnight:
        return nowTime >= startTime or nowTime <= endTime

def filterbytime(data, starttime, endtime): # note that this cannot handle times crossing between midnight
    # -------------------------------
    # 1) Convert dateString -> numpy
    # -------------------------------
    date_strings = [d["dateString"] for d in data]
    dates_full = np.array(date_strings, dtype="datetime64[ms]")

    # Extract calendar dates (e.g., 2025-03-08) and time-of-day offset
    dates_only = dates_full.astype("datetime64[D]")
    time_of_day = dates_full - dates_only

    # ---------------------------------------
    # 2) Time-of-day filter (08:00 to 13:00)
    # ---------------------------------------
    start = np.timedelta64(starttime, "h")  # 08:00
    end = np.timedelta64(endtime, "h")  # 13:00

    time_mask = (time_of_day >= start) & (time_of_day < end)

    # --------------------
    # 3) Apply time filter
    # --------------------
    data_array = np.array(data, dtype=object)  # so we can boolean-index it
    filtered = data_array[time_mask]
    filtered_dates_full = dates_only[time_mask]  # date portion for the filtered items

    # --------------------------
    # 4) Group by calendar date
    # --------------------------
    unique_dates = np.unique(filtered_dates_full)

    groups_by_date = {}
    for day in unique_dates:
        # Create a mask for just this date (within already-filtered data)
        date_mask = (filtered_dates_full == day)

        # Convert it back to a Python list of dicts
        groups_by_date[str(day)] = filtered[date_mask].tolist()

    return groups_by_date



def process_single_row(row, ptIDCol, ptNSCol, startdate, enddate):
    """
    Process a single row of data. Returns a tuple:
       (nightscout_deploy_count, no_data_count, succeeded_count, result_dict_or_None)
    """
    nightscout_deploy = 0
    no_data = 0
    succeeded = 0
    result_entry = None

    ptID = row[ptIDCol]
    ptUUID = row[ptNSCol]

    try:
        # Retrieve Data
        data, response_url = dataretrieve(ptUUID, startdate, enddate)
        # CGM Type
        cgmbrand = cgmtype(data[0]['device'])

        # Filter data by time
        data = filterbytime(data, 7, 13)  # e.g., selects data within 7:00-13:00

        # Quick demonstration printouts (if you need them; remove or wrap in logs for real use)
        # for idx, k in enumerate(data):
        #     if idx == 5: break
        #     print((k, data[k]))

    except Exception:
        # Something prevented data retrieval (e.g., no NS deployed)
        nightscout_deploy += 1
        # Return immediately with counters updated
        return (nightscout_deploy, no_data, succeeded, None)

    # TBR
    TBRcount, readings, datecount = tbrcalc(data, 63, cgmbrand)  # 63 mg/dL = ~3.5 mmol/L
    if TBRcount < 4:
        # We consider this a "no data" scenario for your code
        no_data += 1
        return (nightscout_deploy, no_data, succeeded, None)

    try:
        # Calculate statistics
        percent = dataPercent(readings, cgmbrand)
        avgglucose = average(readings)
        estA1c = GMI(avgglucose)

        succeeded += 1
        # Build a dictionary for this row’s successful result
        result_entry = {
            "ID": ptID,
            "AvgGlucose": avgglucose,
            "GMI": estA1c,
            "Percent": percent,
            "Type": cgmbrand,
            "TBRCount": TBRcount,
            "Data List": datecount,
            "NSURL": ptUUID
        }
    except Exception:
        # Catch unexpected errors in calculations
        no_data += 1

    return (nightscout_deploy, no_data, succeeded, result_entry)

def main():
    # Files
    Snapshot = "gitignore/snapshot20250116.csv"
    NSOutput = "gitignore/osaid.csv"

    # Combine CSVs once (outside parallel loop)
    combinecsv(Snapshot, NSOutput)

    # Read from the merged CSV
    snap = 'gitignore/working.csv'
    with open(snap, mode="r", newline='') as snapdata:
        reader = csv.reader(snapdata)
        headers = next(reader)
        snaplist = list(reader)

    # Find columns of interest
    ptIDCol = headers.index('key')
    ptNSCol = headers.index('ns_uuid')
    enddate = datetime.today().strftime('%Y-%m-%d')
    startdate = startDateCalc(enddate)

    # We’ll accumulate final results here
    final_results = []
    total_nightscout_deploy = 0
    total_no_data = 0
    total_succeeded = 0

    # Create a process pool and map each row
    with concurrent.futures.ProcessPoolExecutor() as executor:
        futures = [
            executor.submit(process_single_row, row, ptIDCol, ptNSCol, startdate, enddate)
            for row in snaplist
        ]

        # As results come in, update counters and store successes
        for future in concurrent.futures.as_completed(futures):
            try:
                nightscout_deploy, no_data, succeeded, result_dict = future.result()
                total_nightscout_deploy += nightscout_deploy
                total_no_data += no_data
                total_succeeded += succeeded

                if result_dict is not None:
                    final_results.append(result_dict)

            except Exception as exc:
                # Catch any weird issues from within a single worker
                print(f"Row worker generated an exception: {exc}")

    # Print final results or handle them as needed
    print("All parallel processing complete.")
    print("Nightscout Undeployed:", total_nightscout_deploy)
    print("No Data Count:", total_no_data)
    print("Succeeded Count:", total_succeeded)
    print("Results:")
    for r in final_results:
        print(r)

    # Finally, write out the aggregated results
    writeresults('results.csv', final_results)

# If you're on Windows, guard your entry point:
if __name__ == "__main__":
    main()