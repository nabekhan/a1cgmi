"""
This script is designed to determine the GMI for a selected period
It should:
1) retrieve data
2) retrive a1c + dates from snapshot
2) calculate the average blood sugar and TIR, TBR, TAR
3) export results
"""

from datetime import datetime, timedelta
import csv
import requests

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
    url = jsonurl(ns_uuid, startDate, endDate)
    auth = ('_cgm', 'queries_')  # Authentication credentials
    response = requests.get(url, auth=auth)
    # Print the final URL being used for the request
    print("Request URL:", response.url)
    response.raise_for_status()  # Check if the request was successful
    data = response.json()
    data = sorted(data, key=lambda d: d['date']) # sort data from first to last date
    return data, response.url

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
    ptGMI = GMI(avgglucose)
    A1cdif = compareA1c(float(A1c), ptGMI)
    TBR = (sum(i < 70 for i in sgv_values))/count*100
    TAR = (sum(i > 180 for i in sgv_values))/count*100
    TIR = 100 - TAR - TBR
    verylow = (sum(i < 54 for i in sgv_values))/count*100
    low = (sum(54 <= i < 70 for i in sgv_values)) / count * 100
    high = (sum(180 < i <= 248.615 for i in sgv_values)) / count * 100
    veryhigh = (sum(i > 248.615 for i in sgv_values))/count*100
    return (cgm, count, percentdata, avgglucose, ptGMI, A1cdif, TBR, TIR, TAR,
            timefluc, timerapid, verylow, low, high, veryhigh)

def process_A1c(row, ptA1cDate, ptA1c, ptNSCol, days, base_columns):
    """Process A1c data and return relevant statistics or empty values if no data."""
    A1c_date = row[ptA1cDate]  # Access by index for lists
    if A1c_date:
        data, response_url = A1cdata(row[ptNSCol], A1c_date, days)
        if data:
            A1c_value = row[ptA1c]  # Access by index for lists
            return GMIstats(data, A1c_value, days) + (A1c_value, A1c_date)
    # Return empty strings for missing data
    return ("",) * len(base_columns)



def main(days = 90):
    with open(f"gitignore/results_"+str(days)+".csv", 'w', newline='', buffering=1) as f:
        writer = csv.writer(f)
        # Make headings
        # Base column headers
        base_columns = ["cgm", "count", "percentdata", "avgglucose", "ptGMI", "A1cdif", "TBR", "TIR", "TAR",
            "timefluc", "timerapid", "verylow", "low", "high", "veryhigh", "A1c_value", "A1c_date"]
        # Generating headers for multiple sets (1, 2, 3)
        headers = ["ID", "link"] + [f"{col}{i}" for i in range(1, 4) for col in base_columns]
        # Initialize results with headers
        results = [headers]
        writer.writerow(results[0])
        # Retrieving Data
        snap = 'gitignore/DPD snapshot (2024-08-11).csv'
        row_count = sum(1 for line in open(snap))
        with open(snap, mode="r") as snapdata:
            readfile = csv.reader(snapdata)
            for index, row in enumerate(readfile):
                print(f"On Row {index}/{row_count} (Days: {days})")
                if index == 0:
                    # Find columns of interest
                    ptIDCol = row.index('dpd_id')
                    ptLinkCol = row.index('link')
                    ptNSCol = row.index('ns_uuid')

                    ptA1c1 = row.index('A1c')
                    ptA1c1date = row.index('A1c_datetime')

                    ptA1c2 = row.index('A1c_previous')
                    ptA1c2date = row.index('A1c_previous_datetime')

                    ptA1c3 = row.index('A1c_3d_most_recent')
                    ptA1c3date = row.index('A1c_3d_most_recent_datetime')

                else:
                    try:
                        print(f"Pt: {str(int((row[ptIDCol]).replace(',', '').replace('.00', '')))}")
                        # List of column mappings for A1c1, A1c2, and A1c3
                        a1c_mappings = [
                            (ptA1c1date, ptA1c1),
                            (ptA1c2date, ptA1c2),
                            (ptA1c3date, ptA1c3)
                        ]

                        results = []

                        # Loop through each A1c mapping and process the data
                        for ptA1cDate, ptA1c in a1c_mappings:
                            results.extend(process_A1c(row, ptA1cDate, ptA1c, ptNSCol, days, base_columns))

                        # Check if any CGM data is present (first three elements correspond to cgm1, cgm2, cgm3)
                        if any(results[i] for i in range(0, len(results), 9)):  # cgm1, cgm2, cgm3 positions
                            output = [
                                int(float(row[ptIDCol].replace(',', ''))),  # ID
                                row[ptLinkCol],  # Link
                                *results  # Unpack all A1c results
                            ]
                            writer.writerow(output)
                            print(output)
                    except Exception as e: print(e)
                        #print("FAILED")
                        #continue
if __name__ == "__main__":
    #main(14)
    #main(90)
    main(60)
    main(30)
