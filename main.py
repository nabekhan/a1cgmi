from a1cgmi import *
from loopstats import *
import pandas as pd
from datetime import date, timedelta

def combinecsv(snapshot, nslist):
    snapshot = pd.read_csv(snapshot)
    snapshot["PATIENT_ID"] = snapshot["link"].str.extract(r"patient_id=(\d+)").astype("int64")
    cols = ["PATIENT_ID"] + [col for col in snapshot.columns if col != "PATIENT_ID"]
    snapshot = snapshot[cols]

    nslist = pd.read_csv(nslist)

    snapshot.rename(columns={snapshot.columns[0]: "key"}, inplace=True)
    nslist.rename(columns={nslist.columns[0]: "key"}, inplace=True)

    # Perform inner join on the first column (assuming no header issues)
    df = snapshot.merge(nslist, on=snapshot.columns[0], how='inner')

    # Convert date columns to datetime (coerce errors so invalid/missing become NaT)
    df['AAPS_date_start'] = pd.to_datetime(df['AAPS_date_start'], errors='coerce')
    df['LOOP_date_start'] = pd.to_datetime(df['LOOP_date_start'], errors='coerce')
    df['iAPS_date_start'] = pd.to_datetime(df['iAPS_date_start'], errors='coerce')

    # Define a helper function that picks the software with the most recent date
    def pick_most_recent_sw(row):
        # Build a list of (software_name, start_date) for each software that is "used" (==1)
        used_softwares = []
        if row['AAPS_AID_y'] == 1 and pd.notnull(row['AAPS_date_start']):
            used_softwares.append(("AAPS", row['AAPS_date_start']))
        if row['Loop_AID_y'] == 1 and pd.notnull(row['LOOP_date_start']):
            used_softwares.append(("Loop", row['LOOP_date_start']))
        if row['iAPS_AID_y'] == 1 and pd.notnull(row['iAPS_date_start']):
            used_softwares.append(("iAPS", row['iAPS_date_start']))

        if not used_softwares:
            return (None, None)  # or return None, or an explicit label like "No AID"

        # Pick the tuple with the max date
        most_recent = max(used_softwares, key=lambda x: x[1])
        return (most_recent[0], most_recent[1]) # replace date with 1 month ago

    # Apply the helper function row by row
    df[["Software", "OSAID startdate"]] = df.apply(pick_most_recent_sw, axis=1, result_type="expand")
    df = df.dropna(subset=['Software'])
    df = df.dropna(subset=['ns_status'])
    df = df[df['ns_status'] != 0]

    # Save the updated DataFrame to a new CSV
    df.to_csv("gitignore/working.csv", index=False)

if __name__ == "__main__":
    #a1cgmi(90)
    Snapshot = "gitignore/snap(2025-03-05).csv"
    NSOutput = "gitignore/modifiedcgmstat.csv"
    combinecsv(Snapshot, NSOutput) # output combined csv with software - gitignore/working.csv
    loopstats('gitignore/working.csv', "cgmnight") #leave start and end time empty to process all data

    #Plan: Set the Loop start date to 1 month ago from now for all people with nightscout accounts. This script will then calc stats.