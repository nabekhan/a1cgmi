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
    count = len(sgv_values)
    avgglucose = average(sgv_values)
    ptGMI = GMI(avgglucose)
    A1cdif = compareA1c(float(A1c), ptGMI)
    TBR = (sum(i < 70 for i in sgv_values))/count*100
    TAR = (sum(i > 180 for i in sgv_values))/count*100
    TIR = 100 - TAR - TBR
    return cgm, percentdata, avgglucose, ptGMI, A1cdif, TBR, TIR, TAR

def main(days = 90):
    with open(f"gitignore/results.csv", 'w', newline='', buffering=1) as f:
        writer = csv.writer(f)
        # Make headings
        results = [["ID", "link",
                    "CGM1", "Percent Data1", "A1c1", "GMI1", "Date1", "Dif1", "TBR1", "TIR1", "TAR1",
                    "CGM2", "Percent Data2", "A1c2", "GMI2", "Date2", "Dif2", "TBR2", "TIR2", "TAR2",
                    "CGM3", "Percent Date3", "A1c3", "GMI3", "Date3", "Dif3", "TBR3", "TIR3", "TAR3"]]
        writer.writerow(results[0])
        # Retrieving Data
        snap = 'gitignore/DPD snapshot (2024-08-11).csv'
        with open(snap, mode="r") as snapdata:
            readfile = csv.reader(snapdata)
            for index, row in enumerate(readfile):
                print(f"On Row {index}")
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
                        if row[ptA1c1date]:
                            A1c1date = row[ptA1c1date]
                            data1, responseurl1 = A1cdata(row[ptNSCol], A1c1date, days)
                            if data1:
                                # Get Stats
                                A1c1 = row[ptA1c1]
                                cgm1, percentdata1, avgglucose1, ptGMI1, A1cdif1, TBR1, TIR1, TAR1 = GMIstats(data1, A1c1, days)
                            else:
                                cgm1 = ""
                                percentdata1 = ""
                                A1c1 = ""
                                ptGMI1 = ""
                                A1c1date = ""
                                A1cdif1 = ""
                                TBR1 = ""
                                TIR1 = ""
                                TAR1 = ""

                        if row[ptA1c2date]:
                            A1c2date = row[ptA1c2date]
                            data2, responseurl2 = A1cdata(row[ptNSCol], A1c2date, days)
                            if data2:
                                # Get Stats
                                A1c2 = row[ptA1c2]
                                cgm2, percentdata2, avgglucose2, ptGMI2, A1cdif2, TBR2, TIR2, TAR2 = GMIstats(data2, A1c2, days)
                            else:
                                cgm2 = ""
                                percentdata2 = ""
                                A1c2 = ""
                                ptGMI2 = ""
                                A1c2date = ""
                                A1cdif2 = ""
                                TBR2 = ""
                                TIR2 = ""
                                TAR2 = ""

                        if row[ptA1c3date]:
                            A1c3date = row[ptA1c3date]
                            data3, responseurl3 = A1cdata(row[ptNSCol], A1c3date, days)
                            if data3:
                                # Get Stats
                                A1c3 = row[ptA1c3]
                                cgm3, percentdata3, avgglucose3, ptGMI3, A1cdif3, TBR3, TIR3, TAR3 = GMIstats(data3, A1c3, days)
                            else:
                                cgm3 = ""
                                percentdata3 = ""
                                A1c3 = ""
                                ptGMI3 = ""
                                A1c3date = ""
                                A1cdif3 = ""
                                TBR3 = ""
                                TIR3 = ""
                                TAR3 = ""

                        if cgm1+cgm2+cgm3:
                            output=([int(float(row[ptIDCol].replace(',', ''))), row[ptLinkCol],
                                            cgm1, percentdata1, A1c1, ptGMI1, A1c1date, A1cdif1, TBR1, TIR1, TAR1,
                                            cgm2, percentdata2, A1c2, ptGMI2, A1c2date, A1cdif2, TBR2, TIR2, TAR2,
                                            cgm3, percentdata3, A1c3, ptGMI3, A1c3date, A1cdif3, TBR3, TIR3, TAR3])
                            writer.writerow(output)
                            print(output)
                    except:
                        continue
if __name__ == "__main__":
    main()
