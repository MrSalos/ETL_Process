import os
import requests
import tempfile
from zipfile import ZipFile
import csv

# START - Paths for new November 2022 data available

# Paths
base_path = "/home/mrsalos/etl_tests/roller_coasters"
source_url = "https://www.kaggle.com/datasets/thedevastator/roller-coaster-data-from-around-the-world/download?datasetVersionNumber=2" 
#this source URL returns a http page and not the zip file of the data I wish to download, but kaggle has an API and there we can connect and download the data needed to do this ETL

# paths were the data is going to be stored the source and the raw
source_path = f"{base_path}/data/source/downloaded_at=2022-11/roller-coaster-data-from-around-the-world.zip"
raw_path = f"{base_path}/data/raw/downloaded_at=2022-11/"

#Create a directory at the "path"  passed as an argument
def create_directory_if_not_exists(path):
    """ 
    Create a directory if it doesn't exists
    """
    try:
        os.makedirs(os.path.dirname(path))
    except:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        print("[Extract] Directory already exists.")

# Write the file obtained to the specified directory
def download_snapshot():
    """
    Download new dataset from the source
    """

    create_directory_if_not_exists(source_path)
    with open(source_path, "wb") as source_roller:
        response = requests.get(source_url)
        print(response.headers['Content-Type'])
        source_roller.write(response.content)

def save_new_raw_data():
    """
    Save new raw data from the source
    """

    # Call for the create_directory function
    create_directory_if_not_exists(raw_path)

    with ZipFile(source_path, "r") as zip_file:
        names_list = zip_file.namelist()
            
        # Check if the file is already created in path
        for file_name in names_list:
            if os.path.isfile(f"{raw_path}/{file_name}"):
                print(f'[Extract] File already exists in {raw_path}/{file_name}')
            else:
                csv_file_path = zip_file.extract(file_name, path=raw_path)

                with open(csv_file_path, 'r') as csv_file:
                    reader = csv.DictReader(csv_file)

                    row = next(reader)
                    print(f"[Extract] File:{file_name}\nFirst row example: {row}")


def main():
    print("[Extract] Start")

    # download_snapshot()
    # print("Snapshot downloaded.")
    save_new_raw_data()

main()