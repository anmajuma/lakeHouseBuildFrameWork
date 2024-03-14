import pandas as pd
import json

filepath = "/home/animesh/python-project/lakeHouseBuildFrameWork/etl-config/datadict.csv"
output_path = "/home/animesh/python-project/lakeHouseBuildFrameWork/etl-config/datadict.json"

df = pd.read_csv(filepath)

# Get the list of all column names from headers
fieldnames = list(df.columns.values)

# Create a multiline json
record_dict = json.loads(df.to_json(orient = "records"))
record_json = json.dumps(record_dict , indent=2)

with open(output_path, 'w') as f:
    f.write(record_json)