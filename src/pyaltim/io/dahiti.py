"""This script holds some functions to download and clean DAHITI Virtual Stations data records for any study area using study area shapefile or geojson file.
As a output of these functions, two different folders are created, DAHITI_Raw and DAHITI_Processed, for ideal management of raw and processed metadata files. A part of this function is adapted from https://dahiti.dgfi.tum.de/en/ api requesting example enlisted in their website.
"""

import geopandas as gpd
import pandas as pd
import glob
import os
import sys
import json
import pprint
import requests
import datetime

def download_DAHITI_stations(path, roi_path):
    """Download Roi intersected virtual stations in the Download folder"""

    # Ensure download directory exists
    download_path = os.path.join(path, 'DAHITI_Raw')
    if not os.path.isdir(download_path):
        os.makedirs(download_path)

    # Function to get bounds from a shapefile or GeoJSON file
    def Roi_bounds(file_path):
        # Check if the file extension is supported
        if not file_path.lower().endswith(('.geojson', '.shp')):
            raise ValueError("Unsupported file format. Only GeoJSON (.geojson) and Shapefile (.shp) formats are supported.")

        # Check if the file exists
        if not os.path.exists(file_path):
            raise ValueError(f"File '{file_path}' not found.")

        # Read the shapefile
        gdf = gpd.read_file(file_path)

        # Get the bounds of the GeoDataFrame
        bounds = gdf.total_bounds

        return bounds

    # Get bounds from the ROI file
    min_lon, min_lat, max_lon, max_lat = Roi_bounds(roi_path)

    # User configuration
    print("Obtain your API key from DAHITI: (https://dahiti.dgfi.tum.de/en/frequently-asked-questions/api-key/)")
    api_key = input("Enter your API key: ")
    output_format = "csv"
    # output_format = input("Enter the output format (ascii/json/netcdf/csv): ")

    args = {
        'api_key': api_key,
        'min_lon': min_lon,
        'max_lon': max_lon,
        'min_lat': min_lat,
        'max_lat': max_lat
    }

    # Define URL for listing targets
    url = 'https://dahiti.dgfi.tum.de/api/v2/list-targets/'

    # Send request as method POST
    response = requests.post(url, data=args)

    results = []
    if response.status_code == 200:
        targets = json.loads(response.text)['data']
        print('Dataset(s) found:', len(targets))

        for target in targets:
            # print(target)

            # Download water level time series
            download_url = 'https://dahiti.dgfi.tum.de/api/v2/download-water-level/'
            download_args = {
                'api_key': api_key,
                'dahiti_id': target['dahiti_id'],
                'format': output_format
            }

            # Construct output file path based on format
            path_output = os.path.abspath(f"{download_path}/{target['dahiti_id']}.{output_format}")

            print('Downloading...', target['dahiti_id'], '->', target['target_name'].encode("utf8"), '(', path_output, ')')

            # Make request to download water level data
            response_download = requests.post(download_url, json=download_args)

            if response_download.status_code == 200:
              my_dict={}
              my_dict['Dahiti_ID']= target["dahiti_id"]
              my_dict['Longitude']= target["longitude"]
              my_dict['Latitude']= target["latitude"]
              results.append(my_dict)
              if output_format in ["csv"]:
                  data = response_download.text
                  with open(path_output, 'w') as output:
                      output.write(data)

            else:
                print('Error: `download-water-level` request failed!')
                data = json.loads(response_download.text)
                pprint.pprint(data)
                # sys.exit(0)
    else:
        print('Error: `list-targets` request failed!')
        data = json.loads(response.text)
        pprint.pprint(data)

    df = pd.DataFrame(results)
    print(df)

    out = os.path.join(path, "DAHITI_Processed")
    if not os.path.isdir(out):
        os.makedirs(out)
    csv_out = out + "/00_DAHITI_metadata_v0.csv"

    df.to_csv(csv_out)

    return(df)





def update_metadata_DAHITI(folder_path):
  '''Updates the existing Metadata file which was created while downloading all csv's. In this updatation it includes station_id, temporal range, min - max data records, lat - long of the stations.'''
  path = folder_path + "/DAHITI_Raw/*.csv"
  file_dir = glob.glob(path)
  file_dir.sort()
  files = file_dir[:]

  Dahiti_DF_list = []
  min_height_data = []
  max_height_data = []
  date_range = []

  for i in range(len(files)):
    id = files[i].split("/")[-1].split(".")[0]
    df = pd.read_csv(files[i],sep=";")
    df["Dahiti_ID"] = id
    min_height_row = df.loc[df['water_level'].idxmin()]
    min_height_data.append(min_height_row[['Dahiti_ID', 'water_level', 'datetime', 'error']])
    max_height_row = df.loc[df['water_level'].idxmax()]
    max_height_data.append(max_height_row[['Dahiti_ID', 'water_level', 'datetime', 'error']])
    oldest_date = df['datetime'].min()
    latest_date = df['datetime'].max()
    date_range.append({'Dahiti_ID': df['Dahiti_ID'].iloc[0], 'oldest_date': oldest_date, 'latest_date': latest_date})

  df1 = pd.DataFrame(min_height_data)
  df2 = pd.DataFrame(max_height_data)
  df3 = pd.DataFrame(date_range)

  MetaDataFrame_DAHITI = pd.merge(df1, df2, on='Dahiti_ID', how='left')
  MetaDataFrame_DAHITI = pd.merge(MetaDataFrame_DAHITI, df3, on='Dahiti_ID', how='left')

  MetaDataFrame_DAHITI["Min_Height"] = MetaDataFrame_DAHITI["water_level_x"]
  MetaDataFrame_DAHITI["MinHeight_Datetime"] = MetaDataFrame_DAHITI["datetime_x"]
  MetaDataFrame_DAHITI["MinHeight_Error"] = MetaDataFrame_DAHITI["error_x"]

  MetaDataFrame_DAHITI["Max_Height"] = MetaDataFrame_DAHITI["water_level_y"]
  MetaDataFrame_DAHITI["MaxHeight_Datetime"] = MetaDataFrame_DAHITI["datetime_y"]
  MetaDataFrame_DAHITI["MaxHeight_Error"] = MetaDataFrame_DAHITI["error_y"]

  path = folder_path + "/DAHITI_Processed/00_DAHITI_metadata_v0.csv"
  file_dir = glob.glob(path)
  file_dir.sort()
  df4 = pd.read_csv(file_dir[0])
  df4["Dahiti_ID"] = df4["Dahiti_ID"].astype(str)
  MetaDataFrame_DAHITI = pd.merge(df4, MetaDataFrame_DAHITI, on='Dahiti_ID', how='left')
  MetaDataFrame_DAHITI.drop(["Unnamed: 0",'water_level_x', 'datetime_x', 'error_x', 'water_level_y','datetime_y', 'error_y'], axis=1, inplace=True)

  out_fn = folder_path + "/DAHITI_Processed/01_DAHITI_metadata_v1_Updated.csv"
  MetaDataFrame_DAHITI.to_csv(out_fn)


  return(MetaDataFrame_DAHITI)