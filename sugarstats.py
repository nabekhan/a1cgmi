"""
This module determines "cgm", "count", "percentdata", "avgglucose", "STD", "ptGMI", "TBR", "TIR (3.9–10 mmol/L)",
                        "TAR","verylow (<3 mmol/L)", "low (3–3.9 mmol/L)", "high (10–13.9 mmol/L)", "veryhigh (>13.8 mmol/L)",
                        "timefluc", "timerapid"

using an input of a nightscout query data
"""

import numpy as np
from data_via_nsuuid import *

# Calculate % Data Based on CGM
def dataPercent(glucoselist, type, days=90):
    if type == "libre":
        percent = len(glucoselist)/(96*days) * 100
    else:
        percent = len(glucoselist)/(288*days) * 100
    return percent

# Calculate the GMI
def GMI(glucoseavg):
    GMI = 3.31 + 0.02392 * glucoseavg
    return(GMI)

# Calculate time in (rapid) fluctuation
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

# Determine CGM type
def cgmtype(device):
    if "lvconnect" in device:
        return "libre"
    else:
        return "dexcom"

# Calculate Stats
def GMIstats(data, days = 90):
    # Attempt to identify the CGM
    try:
        retrievedevice = data[0]['device']
    except:
        retrievedevice = ""
    cgm = cgmtype(retrievedevice)

    # Get percent data based on cgm brand
    percentdata = dataPercent(data, cgm, days)

    # Get sugar readings
    sgv_values, sgv_valuesdt = sugarreadings(data)

    # Determine number of readings
    count = len(sgv_values)

    # Calculate time in fluctuation
    timefluc = timeinfluc(sgv_valuesdt)
    timerapid = timeinfluc(sgv_valuesdt, True)

    # Determine average glucose
    avgglucose = sum(sgv_values) / len(sgv_values)

    # Determine standard deviation
    std = np.std(sgv_values)

    # Determine GMI
    ptGMI = GMI(avgglucose)

    # Determine TBR, TAR, TIR
    TBR = (sum(i < 70.261 for i in sgv_values))/count*100
    TAR = (sum(i > 180.156 for i in sgv_values))/count*100
    TIR = 100 - TAR - TBR

    # Determine very lows, etc.
    verylow = (sum(i < 54.047 for i in sgv_values))/count*100
    low = (sum(54.047 <= i < 70.261 for i in sgv_values)) / count * 100
    high = (sum(180.156 < i <= 250.417 for i in sgv_values)) / count * 100
    veryhigh = (sum(i > 250.417 for i in sgv_values))/count*100

    # Return results
    return (cgm, count, percentdata, avgglucose, std, ptGMI, TBR, TIR, TAR,
            verylow, low, high, veryhigh, timefluc, timerapid)
