from a1cgmi import *
from loopstats import *
import pandas as pd

def combinecsv(csv1, csv2):
    csv1 = pd.read_csv(csv1)
    csv2 = pd.read_csv(csv2)
    csv1.rename(columns={csv1.columns[0]: "key"}, inplace=True)
    csv2.rename(columns={csv2.columns[0]: "key"}, inplace=True)

    # Perform inner join on the first column (assuming no header issues)
    df = csv1.merge(csv2, on=csv1.columns[0], how='inner')

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
            return ""  # or return None, or an explicit label like "No AID"

        # Pick the tuple with the max date
        most_recent = max(used_softwares, key=lambda x: x[1])
        return most_recent[0]

    # Apply the helper function row by row
    df["Software"] = df.apply(pick_most_recent_sw, axis=1)

    # Save the updated DataFrame to a new CSV
    df.to_csv("gitignore/working.csv", index=False)

if __name__ == "__main__":
    #a1cgmi(90)
    Snapshot = "gitignore/snapshot20250116.csv"
    NSOutput = "gitignore/osaid.csv"
    combinecsv(Snapshot, NSOutput) # output combined csv with software - gitignore/working.csv
    loopstats('gitignore/working.csv')