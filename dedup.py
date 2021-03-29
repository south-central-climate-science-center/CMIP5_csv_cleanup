import os
from os.path import isfile
from os import stat, listdir
from datetime import datetime
import pandas as pd

__author__ = "Tyler Pearson <tdpearson>"


if not os.uname().nodename == 'climatedata.oscer.ou.edu':
    print("This script is intended to run on climatedata")
    exit(1)


df = pd.read_csv("/data/static_web/infx/climatedata_updated.csv")


print("Fixing version details...")
def extract_version(row):
    if row.version[0] != "v":
        attributes = row.local_file.split("/")
        return attributes[-2]  # version is included as second to last item in local_file
    return row.version

df.version = df.apply(extract_version, axis=1)


print("Correcting file paths...")
def update_path(row):
    path = row['local_file']
    if not isfile(path):
        if "/data2/synda/sdt/data" in path:
            path = path.replace("/sdt", "")
        elif "/data4/gsmwork/data/" in path:
            path = path.replace("gsmwork", "synda")
            if not isfile(path):
                path = path.replace("/output/", "/output/output/")
                path = path.replace("/output1/", "/output1/output1/")
                path = path.replace("/output2/", "/output2/output2/")
    if not isfile(path):
        raise Exception(f"Could not update path for: {row['local_file']}")
    return path

df['SCCASC_climatedata_path'] = df.apply(update_path, axis=1)


print("Confirming all files exist...")
df['file_exists'] = df['SCCASC_climatedata_path'].apply(lambda x: isfile(x))
assert len(df[~df['file_exists']]) == 0


print("Adding paths for accessing on OSCER Schooner...")
def newpath(item):
    root = item.split("/")[1]
    remaining_path = "/".join(item.split("/")[2:])
    if root == "data":
        fullpath = f"/condo/climatedata3/{remaining_path}"
    else:
        fullpath = f"/condo/climate{root}/{remaining_path}"
    return fullpath

df['OSCER_schooner_path'] = df['SCCASC_climatedata_path'].apply(newpath)
df['local_file'] = df['SCCASC_climatedata_path']


unique = df[~df.duplicated(['filename'], False)]
duplicated = df[df.duplicated(['filename'], False)]
print(f"{len(unique)} unique files")
print(f"{len(duplicated)} duplicated files")


print("Determining which duplicated files to keep...")
mtime = lambda x: datetime.fromtimestamp(stat(x).st_mtime).isoformat()
issues = []
def max_version(grp):
    """ return single record from group """
    
    # max version based on string comparison
    records = grp[grp.version == grp.version.max()]
    
    if len(records) > 1:
        # most recent modified timestamp
        records['file_mtime'] = records.apply(lambda x: mtime(x.local_file), axis=1)
        records = records[records.file_mtime == records.file_mtime.max()]
    
    if len(records) > 1:
        # item with the most files in the same directory
        keep = None
        max_count = 0
        for record in records.to_dict(orient='records'):
            path, file = os.path.split(record['local_file'])
            file_count = len(listdir(path))
            if file_count > max_count:
                keep = record
                max_count = file_count
        records = pd.DataFrame([keep])
    
    if not len(records) == 1:
        issues.append(records.local_file)
    
    if issues:
        raise Exception("Could not determine which record to use for the following: ", issues)
    
    return records
    
deduplicated = duplicated.groupby(['filename'], as_index=False).apply(max_version)
print(f"{len(deduplicated)} deduplicated files")


print("Merging unique and deduplicated...")
cleaned = pd.concat([unique, deduplicated], ignore_index=True)


print("Removing items with time frame after year 2101...")
cleaned['beg_year'] = cleaned.time.str.split("-").apply(lambda x: int(x[0][0:4]))
cleaned = cleaned[cleaned.beg_year < 2101]


print("Sorting...")
cleaned = cleaned.sort_values(['variable', 'model', 'experiment', 'time_frequency', 'filename'])


print("Adding human readable variable names and dimensions...")
df_variable_names = pd.read_csv('variable_name_lookup_table.csv')
df_variable_dimensions = pd.read_csv('variable_dimension_lookup_table.csv')
df_tmp = pd.merge(cleaned, df_variable_names, on='variable')
df_complete = pd.merge(df_tmp, df_variable_dimensions, on='variable')


print("Exporting...")
headers = (
    'variable', 'variable_standard_name', 'variable_long_name', 'institute',
    'model', 'domain', 'dimensions', 'project', 'realm', 'ensemble',
    'experiment', 'time_frequency', 'time', 'version', 'filename', 'size',
    'OSCER_schooner_path', 'SCCASC_climatedata_path', 'checksum_type', 'checksum',
)
df_complete.to_csv("CMIP5_climatedata_full.csv", index=False, columns=headers)

