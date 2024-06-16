from datetime import datetime
import json
import time
import requests
import pandas as pd
from sqlalchemy import create_engine

# url for downloading and endpoint url
url = 'https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2023/discogs_20230101_labels.xml.gz'
endpoint = "https://api.discogs.com/labels/{}/releases"

# Open token file
with open(r"C:\Users\rdog0\DiscogsCredentials.json") as f:
    CFG = json.load(f)


# Create fucntion for extracting the xml
def extract():
    return pd.read_xml('https://discogs-data-dumps.s3-us-west-2.amazonaws.com/data/2023/discogs_20230101_labels.xml.gz')

# Get api label data
def get_api_labels(label_id, page):                         # pass through label_id and page(page the # of results to retrieve
    response = requests.get(endpoint.format(label_id),
        params={'token':CFG['token'],
                'page':page,
                'per_page':25
               })
    if response.status_code != 200:
            raise Exception("Failed to get token from Discogs API")
    # sleep for 1 sec before making another request
    time.sleep(1)
    # return the response in json format
    return response.json()

# Define function to extract total releases from the API response
def get_total_releases(response):
    return response['pagination']['items']


# Define function to extract total years
def extract_years(response):
    return[x['year'] for x in response['releases']]


# Find min & max years released
def get_min_max_year(years):
    # use lambda to filter years that can't be valid (pre 1900 and post 2023)
    years = list(filter(lambda year: (year >= 1990) & (year <= 2023),years))
    # return the min and max yrs from the list
    return (min(years), max(years))

# Find total releases
def get_all_label_data(label_id):
    label_data = {'id': label_id}

    # get initial page of label release info
    response = get_api_labels(label_id, 1)
    # pull function that gets all releases
    label_data['total_release_count'] = get_total_releases(response)
    # pull function that extracts years
    label_data['years'] = extract_years(response)

    # determine how many pages of release to iterate through
    num_pages = response['pagination']['pages']

    # for loop to iterate through each subsequesnt release page
    for page_num in range(2, num_pages +1):               # start at 2 to ensure the first page of releases is fetched seperately
        response = get_api_labels(label_id, page_num)
        label_data['years'] += extract_years(response)

    # determine maxs and mins from all years
    label_data['min_release_year'], label_data['max_release_year'] = get_min_max_year(label_data['years'])
    # remove list of years bc we don't need a seperate list 
    label_data.pop('years')

    return label_data


# Define function for ALL label data to create our dataframe
# set attempts so the API doesnt keep running if there's an error
total_attempts = 5

def get_all_additional_label_data(label_ids):
    additional_label_data = []

    count = 1
    attempt = 0
    # append retrieved data into a list
    for label_id in label_ids:
        # call get_all_label_data to use the release info
        try:
            additional_label_data.append(get_all_label_data(label_id))
        except:
            print(f"{datetime.now()}|Failed to add additional data for label: {label_id}")
            if attempt < total_attempts:
                # increase attempts until max is reached (5)
                attempy += 1
                continue
            else:
                break
        # increase count and keep track of total labels successfully fetched & processed
        count += 1   
        # for every 30 labels...print a message
        if count % 30 == 0:
            print(f"{datetime.now()}|Added additional label data for {count} labels")
    # return dataframe
    return pd.DataFrame(additional_label_data)


# Define function for ALL label data to create our dataframe
# set attempts so the API doesnt keep running if there's an error
total_attempts = 5

def get_all_additional_label_data(labels_id):
    additional_label_data = []

    count = 1
    attempt = 0
    # append retrieved data into a list
    for label_id in label_ids:
        # call get_all_label_data to use the release info
        try:
            additional_label_data.append(get_all_label_data(label_id))
        except:
            print(f"{datetime.now()}|Failed to add additional data for label: {label_id}")
            if attempt < total_attempts:
                # increase attempts until max is reached (5)
                attempt += 1
                continue
            else:
                break
        # increase count and keep track of total labels successfully fetched & processed
        count += 1   
        # for every 30 labels...print a message
        if count % 30 == 0:
            print(f"{datetime.now()}|Added additional label data for {count} labels")
    # return dataframe
    return pd.DataFrame(additional_label_date)
        


# function to load our database
db_name = 'music_records'
db_user = 'postgres'
db_pass = 'hello'
db_host = 'localhost'
db_port = '5432'

def load_db(df):
    # define our engine
    engine = create_engine(f"postgresql+psycopg2://{db_user}:{db_pass}@{db_host}/{db_name}")
    # df into sql
    df.to_sql(name='labels', con=engine, index=False, if_exists='replace')


# functions to actually execute 
def main():
    df = extract()
    df = transform(df)
    load(df)

if __name__=='__main__':
    main()





















