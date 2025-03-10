from datetime import time
from dateutil import parser
import urllib.request
from main import *

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

def writeresults(output, list):
    with open(output, mode="r+") as output:
        fieldnames = ["ID", "StartDate", "AvgGlucose", "GMI", "Percent", "Type", "TBRCount", "NSURL"]
        writer = csv.DictWriter(output, fieldnames=fieldnames)
        writer.writeheader()
        for row in list:
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



def main():
    # Collecting Failed Statistics
    nightscoutDeploy = 0
    noData = 0
    succeeded = 0

    # Combining Data
    Snapshot = "gitignore/snapshot20250116.csv"
    NSOutput = "gitignore/osaid.csv"
    combinecsv(Snapshot, NSOutput)
    snap = 'gitignore/working.csv'
    with open(snap, mode="r") as snapdata:
        readfile = csv.reader(snapdata)
        headers = next(readfile)
        snaplist = list(readfile)

        # Find columns of interest
        ptIDCol = headers.index('key')
        ptNSCol = headers.index('ns_uuid')
        enddate = datetime.today().strftime('%Y-%m-%d')
        startdate = startDateCalc(enddate)

        # Skip field headers and iterate through each row
        results = []
        for row in snaplist:
            ptID = row[ptIDCol]
            print(ptID)
            ptUUID = row[ptNSCol]
            print(row)

            try:
                #Retrieve Data
                data, response_url = dataretrieve(ptUUID, startdate, enddate)
                # CGM Type
                cgmbrand = cgmtype(data[0]['device'])
                print(cgmbrand)
                data = filterbytime(data, 7, 13) # selects data within selected time
                for idx, k in enumerate(data):
                    if idx == 5: break
                    print((k, data[k]))
            except:
                print("Nightscout not deployed? Skipping. . .")
                nightscoutDeploy = nightscoutDeploy + 1
                continue

            # TBR
            TBRcount, readings, datecount = tbrcalc(data, 63, cgmbrand) # <3.5 mmol/L
            if TBRcount < 4:
                print("Count less than 3! Skipping . . .")
                continue
            try:
                print(readings)
                # Percent
                percent = dataPercent(readings, cgmbrand)

                # Glucose Avg
                avgglucose = average(readings)

                # GMI
                estA1c = GMI(avgglucose)

                # Add to results
                print(ptID)
                print(cgmbrand)
                succeeded = succeeded + 1
                results.append({"ID": ptID, "StartDate": startdate, "AvgGlucose": avgglucose,"GMI": estA1c, "Percent": percent, "Type": cgmbrand, "TBRCount": TBRcount, "Data List": datecount, "NSURL": ptUUID})
            except:
                continue

        # Output
        print(results)
        print("Nightscout's Undeployed: "+ str(nightscoutDeploy) + " | No Data: " + str(noData) + " | Succeeded: " + str(succeeded))
        writeresults('results.csv', results)


# Note: not selecting time range needed
if __name__ == "__main__":
    main()
