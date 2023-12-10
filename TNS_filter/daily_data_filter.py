import csv
import json
import os
import time
import typing
from collections import OrderedDict
from datetime import datetime

import pandas as pd
import requests
import zipfile

URL_TO_DAILY_CSV = 'https://sandbox.wis-tns.org/system/files/tns_public_objects/tns_public_objects.csv.zip'

TNS_BOT_ID          = "167012"
TNS_BOT_NAME        = "trial_bot"
TNS_API_KEY         = "be3ee6320999824f413f65be89d22e27684f0653"
wanted_date =  datetime(2023, 12, 3)

def set_bot_tns_marker() -> str:
    tns_marker = 'tns_marker{"tns_id": "' + str(TNS_BOT_ID) + '", "type": "bot", "name": "' + TNS_BOT_NAME + '"}'
    return tns_marker

def get_data_from_tns(tns_marker:str) -> any:
    headers = {
    'user-agent': tns_marker
    }
    data = {
    'api_key': TNS_API_KEY
    }
    response = requests.post(URL_TO_DAILY_CSV, headers=headers, data=data)

    # Check the response status
    if response.status_code == 200:
        # The content of the response (the downloaded file) can be accessed using response.content
        print('data downloaded successfully.')
        return response.content
    else:
        print(f'Error downloading file. Status code: {response.status_code}')
        print(response.text)  # Print the response content for further inspection if needed
        return None

def response_data_to_df(data: any) -> pd.DataFrame:
    with open('tns_public_objects.zip', 'wb') as f:
            f.write(data)
        # extracts the file and creates a DF from it
    with zipfile.ZipFile('tns_public_objects.zip', 'r') as zip_file:
        with zip_file.open('tns_public_objects.csv') as csv_file:
            df = pd.read_csv(csv_file,sep=",",index_col=False, skiprows=1, parse_dates=[20])
            return df

def last_modified_filter(df: pd.DataFrame) -> pd.DataFrame:
    
    rslt_df = df[df['lastmodified'] > wanted_date]
    modified_df = rslt_df[rslt_df['type'].notna()]
    return modified_df

def types_filter(df: pd.DataFrame, attr1: str, attr2:str) ->pd.DataFrame:
    filtered_df = df[df['type'].str.contains(attr1) | df['type'].str.contains(attr2, case=False)]
    return filtered_df

def export_to_csv(df: pd.DataFrame):
    filtered_data_path = f'daily_filtered_data.csv'
    df.to_csv(filtered_data_path, index=False)


def main():
    tns_marker = set_bot_tns_marker()
    data = get_data_from_tns(tns_marker)
    if data: #if not - there is an error in the response
        df = response_data_to_df(data)
        modified_df = last_modified_filter(df)
        if not modified_df.empty:
            typed_df = types_filter(modified_df,'SN','TDE')
            export_to_csv(typed_df)
        else: print(f"there were no SN or TDE classifies since {wanted_date}")

   

if __name__ == "__main__":
    main()
