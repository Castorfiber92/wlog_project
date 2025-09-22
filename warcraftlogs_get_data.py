# Importing packages
import os
import json
import logging
import requests
import datetime
from datetime import datetime
import traceback

import pyarrow as pa
import pyarrow.parquet as pq

import numpy as np
import pandas as pd

from dotenv import load_dotenv, set_key

# Set directories
CACHE_FILE ='processed_codes.json'
RAW_DATA_DIR = 'weekly_raw_data'
PROCESSED_DATA_DIR = 'all_reports_parquet_dataset'

# Setting up the API
authURL = "https://www.warcraftlogs.com/oauth/authorize"
tokenURL= "https://www.warcraftlogs.com/oauth/token"
api_key = os.getenv('client_secret')

# Setting up logging
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s - %(name)s -%(levelname)s -%(message)s",
                    handlers=[logging.StreamHandler()])

logger = logging.getLogger(__name__)

# Functions for handling the token needed for authorization. 

def read_token(token_name='WARCRAFTLOGS_TOKEN'):
    """
    Reads a token from a .env file.

    This function first loads environment variables from a .env file
    if it exists. It then attempts to retrieve the specified token
    from the environment.

    Args:
        token_name (str): The name of the environment variable that holds
                          the token.

    Returns:
        str or None: The token string if found, otherwise None.
    """
    load_dotenv()
    token = os.getenv(token_name)

    if token is None:
        print(f"Error: The token '{token_name}' was not found in the .env file.")
        logger.info(f"Error: The token '{token_name}' was not found in the .env file.")
        return None

    return token

def store_token(token, token_name='WARCRAFTLOGS_TOKEN'):
    """
    Saves a new token to the .env file.

    Args:
        token (str): The token to be saved.
        token_name (str): The name of the environment variable.
    """
    dotenv_path = os.path.join(os.getcwd(), '.env')
    set_key(dotenv_path, token_name, token)

    print(f"Successfully saved new token to the .env file under key '{token_name}'.")
    logger.info(f"Successfully saved new token to the .env file under key '{token_name}'.")

def get_new_token(client_id, client_secret):
    """
    Gets a new access token from the Warcraft Logs API using the Client Credentials flow.
    If successful, it saves the new token to the .env file.

    Args:
        client_id (str): The public client ID for your application.
        client_secret (str): The confidential client secret for your application.

    Returns:
        str or None: The new access token string if successful, otherwise None.
    """
    url = "https://www.warcraftlogs.com/oauth/token"
    data = {'grant_type': 'client_credentials'}

    try:
        response = requests.post(url, data=data, auth=(client_id, client_secret))

        token_data = response.json()
        access_token = token_data.get('access_token')

        if access_token:
            print("Successfully retrieved a new access token.")
            logger.info("Successfully retrieved a new access token.")
            store_token(access_token)
            return access_token
        else:
            print("Error: Access token not found in the API response.")
            logger.info("Error: Access token not found in the API response.")
            return None

    except requests.exceptions.RequestException as e:
        print(f"An error occurred while getting a new token: {e}")
        return None
    
# Base function for making querys
def make_query(token: str, query: str) -> dict:
    """
    Makes a GraphQL query to the Warcraft Logs API using the provided access token.

    Args:
        token (str): The access token to use for authorization.
        query (str): The GraphQL query string.

    Returns:
        dict or None: The JSON response data if successful, otherwise None.
    """
    url = "https://www.warcraftlogs.com/api/v2/client"
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/json"
    }
    data = {'query': query}

    try:
        response = requests.post(url, headers=headers, json=data)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the GraphQL query: {e}")
        return None

# Functions for getting fightIDs. 
# One report can contain multiple fights (one fight = one whole dungeon-run)

def make_fightID_query(report_code: str) -> str:
    """
    Creates a query-string for getting information about the fights in the report.

    Args:
        report_code (str): Uniqe code for the report used in the query.

    Returns:
        query (str): Query string to be used in a api call.
    """
    query = f"""query PlayerDungeonMetrics{{
                                            reportData{{
                                                report(code: "{report_code}"){{
                                                        title
                                                        fights(translate: true, difficulty: 10) {{
                                                            id
                                                            friendlyPlayers
                                                            gameZone{{
                                                                name
                                                                }}
                                                            difficulty
                                                            keystoneLevel
                                                        }}
                                                    }}
                                                }}
                                            }}""" 
    return query

def get_fightID(token: str, report_code: str) -> pd.DataFrame:
    """ 
    Makes the API-call with the query to get data about the fights in the report.

    Args:
        token (str): The access token to use for authorization.
        report_code (str): The reportcode for a report on warcraftlogs.

    Returns:
        df (pd.DataFrame): A dataframe with the ID of the diffrent fights in the report.
    """
    test_query = make_query(token, make_fightID_query(report_code))
    df = pd.json_normalize(test_query, record_path=['data', 'reportData', 'report', 'fights'])
    return df

def clean_fightID_df(dataframe):
    """ 
    Cleans the fightID_df dataframe.

    This is what limits the data to dungeons (where you are 5 players) and not raids. 

    Args:
        dataframe (pd.DataFrame): The fightsID_df
    
    Returns: 
        df_filtered (pd.DataFrame): A cleaned version of the fightsID dataframe.
    """
    df = dataframe
    # Only take the fights where you have 5 players
    mask = df['friendlyPlayers'].apply(len) == 5
    df_filtered = df[mask]
    return df_filtered

#Functions for adding gameID data (name, report ID, game ID) for the report.

def make_gameID_query(report_code: str) -> str:
    """
    Creates a query-string for getting information about the character ID's in the report.

    Args:
        report_code (str): Uniqe code for the report used in the query.

    Returns:
        query (str): Query string to be used in a api call.
    """
    query = f"""query PlayerDungeonMetrics{{
                                            reportData{{
                                                report(code: "{report_code}"){{                        
                                                    masterData{{
                                                        actors(type: "Player"){{
                                                            name
                                                            gameID
                                                            id
                                                            }}
                                                        }}
                                                    }}
                                                }}
                                            }}""" 
    return query

def get_gameID(token: str, report_code: str) -> pd.DataFrame:
    """ 
    Makes the API-call with the query to get data about the character ID's.

    Args:
        token (str): The access token to use for authorization.
        report_code (str): The reportcode for a report on warcraftlogs.

    Returns:
        df_gameID (pd.DataFrame): A dataframe with the characters name, gameID and report id.
    """
    gameID_query = make_query(token, make_gameID_query(report_code))
    df_gameID = pd.json_normalize(gameID_query, record_path=['data', 'reportData', 'report', 'masterData', 'actors'])
    return df_gameID

# Functions for damage and healing in a fight.

def make_damage_query(report_code: str, fight_id: str) -> str:
    """
    Creates a query-string for getting information about the damage in the report.

    Args:
        report_code (str): Uniqe code for the report used in the query.

    Returns:
        query (str): Query string to be used in a api call.
    """
    query = f"""query PlayerDungeonMetrics{{
                                            reportData{{
                                                report(code: "{report_code}"){{                        
                                                    table(fightIDs: [{fight_id}], dataType: DamageDone, hostilityType: Friendlies)
                                                    }}
                                                }}
                                            }}""" 
    return query

def make_healing_query(report_code: str, fight_id: int) -> str:
    """
    Creates a query-string for getting information about the healing in the report.

    Args:
        report_code (str): Uniqe code for the report used in the query.

    Returns:
        query (str): Query string to be used in a api call.
    """
    query = f"""query PlayerDungeonMetrics{{
                                            reportData{{
                                                report(code: "{report_code}"){{                        
                                                    table(fightIDs: [{fight_id}], dataType: Healing, hostilityType: Friendlies)
                                                    }}
                                                }}
                                            }}""" 
    return query

def get_damage_and_healing(token: str, report_code: str, fight_ID: int) -> pd.DataFrame:
    """
    Uses a token,reportcode and the fight ID for warcraftlogs to get data for damage and healing.

    Args:
        token (str): The access token to use for authorization.
        report_code (str): The reportcode for a report on warcraftlogs.
        fight_ID (int): A number indicating what fight in the report that was used.
    Returns:
        DataFrame with data for damage and healing.
    """
    #Damage part
    damage_query = make_query(token, make_damage_query(report_code, fight_ID))
    df_dmg_temp = pd.json_normalize(damage_query, record_path=['data', 'reportData', 'report', 'table', 'data', 'entries'])
    dmg_columns = ['name', 'type', 'itemLevel', 'total']
    df_damage = df_dmg_temp[dmg_columns]
    df_damage.columns = ['name', 'class', 'ilvl', 'Dps']
            
    #healing part
    healing_query = make_query(token, make_healing_query(report_code, fight_ID))
    df_heal_temp = pd.json_normalize(healing_query, record_path=['data', 'reportData', 'report', 'table', 'data', 'entries'])
    heal_columns = ['name', 'total']
    df_heal = df_heal_temp[heal_columns]
    df_heal.columns = ['name', 'Healing']
            
    #Merge them
    merged_df = pd.merge(df_heal, df_damage, on='name')
    #Reorder columns
    healing_column = merged_df.pop('Healing')
    merged_df.insert(4, 'Healing', healing_column)

    return(merged_df)

# Functions for getting the starting time of the report.


def make_report_start_query(report_code: str) -> str:
    """
    Creates a query-string for getting information about the start time in the report.

    Args:
        report_code (str): Uniqe code for the report used in the query.

    Returns:
        query (str): Query string to be used in a api call.
    """
    query = f"""query PlayerDungeonMetrics{{
                                            reportData{{
                                                report(code: "{report_code}"){{                        
                                                    startTime
                                                    }}
                                                }}
                                            }}""" 
    return query

def get_report_start(token: str, report_code: str) -> int:
    """ 
    Get the starting time of the report.

    Args:
        token(str): The token for making the API call
        report_code(str): The report code for the report we're looking at.

    Returns:
        start_time(int): The startingtime as an int (in UNIX-format)
    """
    date_query = make_query(token, make_report_start_query(report_code))
    start_time = int(round((date_query['data']['reportData']['report']['startTime'] / 1000)))
    return start_time

# Functions for getting the starting time for a fight. 
# Also uses UNIX, but with 0 as the reports starting time.

def make_fight_start_query(report_code: str, id: int) -> str:
    """
    Creates a query-string for getting information about the starttime for a fight in the report.

    Args:
        report_code (str): Uniqe code for the report used in the query.
        id (int): Tells the program what fight in the report we're looking at. 
    Returns:
        query (str): Query string to be used in a api call.
    """
    query = f"""query PlayerDungeonMetrics{{
                                            reportData{{
                                                report(code: "{report_code}"){{                        
                                                    fights(fightIDs: {id}){{
                                                        id
                                                        startTime
                                                        }}
                                                    }}
                                                }}
                                            }}""" 
    return query

def get_fight_start(token: str, report_code: str, id: int, unix_report_start: int) -> float:
    """ 
    Makes the API call to get the starting time for a fight. 
    The starting time will be in UNIX format, in relation to the starttime of the report.
    Removes the millisecond part from UNIX.

    Args: 
        token (str): token for making the API call
        report_code (str): the code for the report we're looking at.
        id (int): Tells the program what fight in the report we're looking at.
        unix_report_start (int): The startingtime for the report.
    """
    date_query = make_query(token, make_fight_start_query(report_code, id))
    unix_fight = (date_query['data']['reportData']['report']['fights'][0]['startTime'])/1000
    
    # Adds the starttime of the fight to the starttime of the report so we get a correct conversion later.
    unix_fight_start = unix_report_start + unix_fight
    return unix_fight_start

# Converts UNIX to datetime
def convert_time(time_unix: float) -> datetime:
    """ 
    Converts the time from UNIX to a datetime object.

    Args: 
        time_unix (float): A number representing the time

    Returns:
        time (datetime): The date and time as a datetime object. 
    """
    time = datetime.fromtimestamp(time_unix)
    return time

# Get's the name of the dungeon
def get_dungeon_name(fightID: int, df: pd.DataFrame) -> str:
    """ 
    Get the name of the dungeon for the corresponding fight.

    Args:
        fightID (int): Identifies what fight we want the name for.
        df (dataFrame): dataFrame with the data we need.

    Returns:
        dungeon_name (str): The name of the dungeon.

    """
    mask = df['id'] == fightID
    dungeon_name = df.loc[mask, 'gameZone.name'].squeeze()
    if pd.isna(dungeon_name):
        return ("No name found")
    else:
        return dungeon_name
    
# Functions for deaths in a fight. 

def make_deaths_query(report_code: str, fight_id: int) -> str:
    """
    Creates a query-string for getting information about the deaths in the report.

    Args:
        report_code (str): Uniqe code for the report used in the query.

    Returns:
        query (str): Query string to be used in a api call.
    """
    query = f"""query PlayerDungeonMetrics{{
                                            reportData{{
                                                report(code: "{report_code}"){{                        
                                                    table(fightIDs: [{fight_id}], dataType: Deaths, hostilityType: Friendlies)
                                                    }}
                                                }}
                                            }}""" 
    return query


def get_deaths(token: str, report_code: str, fight_id: int) -> pd.DataFrame:
    """ 
    Makes the API call for getting information about the deaths during the fight. 
    If there are no deaths returns a empty dataframe.

    Args:
        token (str): token for making the API call
        report_code (str): the code for the report we're looking at.
        id (int): Tells the program what fight in the report we're looking at.

    Returns: 
        df_deaths (pd.DataFrame): A dataframe with information about the deaths during the fight.
    """
    deaths_query = make_query(token, make_deaths_query(report_code, fight_id))
    df_deaths_temp = pd.json_normalize(deaths_query, record_path=['data', 'reportData', 'report', 'table', 'data', 'entries'])
    if 'name' in df_deaths_temp.columns:
        deaths_columns = ['name']
        df_deaths = df_deaths_temp[deaths_columns]
        return df_deaths
    else:
        return pd.DataFrame(columns=['name'])


def make_name_id_death_df(deaths_dataframe: pd.DataFrame, name_id_dataframe: pd.DataFrame) -> pd.DataFrame:
    """ 
    Creates a new dataframe where the deaths for each character is summarized and the ID of each character is shown.

    Args:
        deaths_dataframe(pd.DataFrame): dataframe with the deaths
        name_id_dataframe(pd.DataFrame): dataframe with information about the characters name and ID.
    
    Returns:
        df_name_id_deaths(pd.DataFrame): dataframe with the name of the character, it's report ID and number deaths.
    """
    # Counts the deaths in the dataframe
    death_counts = deaths_dataframe.value_counts()

    # Merges the dataframes
    df_name_id_deaths = name_id_dataframe.merge(death_counts, how='outer', on='name')

    # Changes NaN to 0
    df_name_id_deaths = df_name_id_deaths.fillna(0)

    # Renames the columns
    df_name_id_deaths = df_name_id_deaths.rename(columns={'count':'deaths'})

    # Makes sure the death's columns are ints. 
    df_name_id_deaths['deaths'] = df_name_id_deaths['deaths'].astype(int)
    
    return df_name_id_deaths

# Functions for managing report codes

def make_report_codes_query(user_id: int) -> str:
    """ 
    Makes the API call to get new codes for the week. 

    Args:
        user_id (int): Int for representing the user whom uploaded the reports.

    Returns:
        query (str): String with the query
    
    """
    query = f"""query PlayerDungeonMetrics{{
                                            reportData{{
                                                reports(userID: {user_id}, limit: 100){{                        
                                                    data{{
                                                        code
                                                        title
                                                        startTime
                                                        }}
                                                    }}
                                                }}
                                            }}""" 
    return query

def get_report_codes(token: str) -> set:
    """ 
    Fetches new codes for the week.

    Args: 
        token (str): token for making the api call.
    
    Returns:
        codes (set): a set with report codes.
    
    """
    codes = []
    user_ids = ['297125', '291792']

    date = datetime.now()
    date_UNIX = date.timestamp()
    date_UNIX_past = date_UNIX - (60*60*24*7)

    for id in user_ids:

        report_codes = make_query(token, make_report_codes_query(id))
        df = pd.json_normalize(report_codes, record_path=['data', 'reportData', 'reports', 'data'])
        df['startTime'] = df['startTime'] / 1000
        mask = df['startTime'] > date_UNIX_past
        filtered_df = df[mask]
        new_codes = filtered_df['code'].tolist()
        codes.extend(new_codes)
    codes = set(codes)    
    return codes


def check_codes(new_codes: list, old_codes: set) -> list:
    """
    Compares preivous codes with the new list to check for duplicates.

    Args:
        new_codes (list): Set with all the new codes as strings.
        old_codes (list): Set with all the old codes as strings.
    Returns:
        new_codes (list): The list with new codes but with duplicates removed.
    """
    set1 = set(new_codes)
    set2 = set(old_codes)
    
    common_codes = set1.intersection(set2)
    for code in common_codes:
        if code in new_codes:
            new_codes.remove(code)

    return new_codes

def load_cache_codes() -> set:
    """
    Loads used codes from a cache file.

    Returns:
        set (set): a set with the old codes.
    """
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'r') as f:
                return list(json.load(f))
        except (IOError, json.JSONDecodeError) as e:
            print(f"Error loading cache file: {e}")
            # Start with an empty cache if there's an error
            return list()
    return list()

def save_cache_codes(codes: set):
    """
    Saves used codes to a cache file.

    Args:
        codes (list): a list with all codes.
    """
    codes_as_list = list(codes)
    try:
        with open(CACHE_FILE, 'w') as f:
            json.dump(codes_as_list, f, indent=4)
    except IOError as e:
        print(f"Error saving cache file: {e}")

# Functions for managing the data

# Saves the weekly reports as JSON in the raw data folder

def save_weekly_data(code: str, input_data: pd.DataFrame):
    """ 
    Saves data from a report to a JSON file used for temporary storage.

    Args:
        code (str): code for a single report as a string
        input_data (dataFrame): dataframe with the data for the report

    """
    # Create folder for temporary storage
    if not os.path.exists(RAW_DATA_DIR):
        os.makedirs(RAW_DATA_DIR)

    file_path = os.path.join(RAW_DATA_DIR, f"{code}.json")

    if os.path.exists(file_path):
        print(f"JSON file for '{code}' already exists. Skipping API call.")    

    # Converts the data to a list for JSON storage. Since the dataframe contains datetime data
    # we use json_string to conserve it. 
    json_string = input_data.to_json(orient='records', date_format='iso')
    data_list = json.loads(json_string)

    data = {"report_code": code, "Data": data_list}
    with open(file_path, 'w') as f:
        json.dump(data, f)


def append_weekly_data_to_dataset():
    """ 
    Takes the JSON-files in the temporary folder and appends to a parquet dataset.
    """
    #Empty list to which we append data
    all_data = []

    # Check so there is data
    if not os.path.exists(RAW_DATA_DIR):
        print("Weekly raw data directory does not exist. No new data to append.")
        return
    
    for filename in os.listdir(RAW_DATA_DIR):
        if filename.endswith('.json'):
            file_path = os.path.join(RAW_DATA_DIR, filename)
            with open(file_path, 'r') as f:
                all_data.append(json.load(f))

    if not all_data:
        print("No new data to append")
        return
    
    # Cleaning the data so it's easer to handle when opening it in the future.
    df_new = pd.DataFrame(all_data)
    df_exploded = df_new.explode('Data')
    df_final = pd.json_normalize(df_exploded['Data'])

    # Add a column to the data for indicating when it was added to the dataset. 
    current_date = datetime.now().strftime('%Y-%m-%d')
    df_final['runDate'] = current_date
    print(f"Loaded {len(df_new)} new reports into a DataFrame.")

    # Convert the pandas DataFrame to a PyArrow Table
    table_new = pa.Table.from_pandas(df_final)

    # Append the new data to the Parquet dataset
    print(f"Appending new data to the '{PROCESSED_DATA_DIR}' dataset...")
    pq.write_to_dataset(table_new, PROCESSED_DATA_DIR,
                        partition_cols=['runDate'],
                        basename_template='part-{i}.parquet')
    
    print("New data successfully appended to the Parquet dataset.")

    # Clean up the weekly JSON files
    for filename in os.listdir(RAW_DATA_DIR):
        os.remove(os.path.join(RAW_DATA_DIR, filename))
    os.rmdir(RAW_DATA_DIR)
    print("Cleaned up weekly raw data directory.")


def look_at_dataset():
    """ 
    Use to load the dataset (used when making the script in jupyter notebook)

    Returns:
        df (dataframe): dataframe with all the data. 
    """
    file_path = 'all_reports_parquet_dataset/'

    df = pd.read_parquet(file_path)
    return df

# Main script
def main():
     
    logger.info("Starting the script")

    # Reads the token for making API calls
    token = read_token()
    if not token:
        print("No token found, fetching new one...")
        client_id = os.getenv('CLIENT_ID')
        client_secret = os.getenv('CLIENT_SECRET')
        response = get_new_token(client_id, client_secret)
        token = response.json().get("WARCRAFTLOGS_TOKEN")
        
    logger.info("Autherization complete")

    # Load cached reportcodes
    old_codes = load_cache_codes()

    if token:
        try:
            # Load new codes (function not done yet, manually add)
            list_of_codes = get_report_codes(token)

            # Remove codes that were present in the cache
            weekly_codes = check_codes(list_of_codes, old_codes)

            # Counter for printing the progress of the report codes. 
            counter_1 = 1

            # Start going through the report-codes from the list. 
            for code in weekly_codes:

                #Used for printing the progress.
                number_of_codes = len(weekly_codes)
                
                # Creates a JSON file in the short storages folder, will be removed if program runs successfully.
                file_path = os.path.join(RAW_DATA_DIR, f"{code}.json")
                if os.path.exists(file_path):
                        print(f"JSON file for '{code}' already exists. Skipping API call.")
                        continue

                # Get the name, id and gameID for characters in the report.
                gameID = get_gameID(token, code)

                #Get the starting time of the report.
                unix_report_start = get_report_start(token, code)

                #Get fightID for diffrent runs and then create a dict with fightID as key and playerID's for that fightID as values.
                df_fightID = get_fightID(token, code)
                df_fightID = clean_fightID_df(df_fightID)
                fightID_dict = dict(zip(df_fightID['id'], df_fightID['friendlyPlayers']))
                
                
                # Create empty list of dataframes and a empty dataframe used in the fight's loop.
                list_of_dataframes = []
                df_weekly = pd.DataFrame()

                # Used for printing the progress with the fights
                counter_2 = 1

                # Start going through each fight in the report (reminder: a fight equals a whole dungeon-run)
                for key in fightID_dict:
                    
                    # Used for printing the progress of the fights.
                    number_of_fights = len(fightID_dict)

                    # Get the start time of the fight
                    start_time_fight = convert_time(get_fight_start(token, code, key, unix_report_start))

                    # Get the player names and ids for the specific run.
                    df_name_id = gameID[gameID['id'].isin(fightID_dict[key])]

                    # Get healing and damage for the players in the run.
                    df_dmg_healing = get_damage_and_healing(token, code, key)

                    # Get a list of all deaths for the run. Sum them up and merge with the name_id dataframe.
                    # Players with no deaths will be missing in death_counts, so fillna(0) is used to set the number to 0 instead of NaN.
                    df_deaths = get_deaths(token, code, key)
                    df_name_id_deaths = make_name_id_death_df(df_deaths, df_name_id)

                    #Merge the two dataframes on name.
                    df_complete = pd.merge(df_name_id_deaths, df_dmg_healing, how='outer', on='name')

                    # Add the dungon name and starttime to the dataframe
                    dungeon_name = get_dungeon_name(key, df_fightID)
                    df_complete['DungeonName'] = dungeon_name
                    df_complete['StartTime'] = start_time_fight

                    # Add the dataframe to a list for future merge
                    list_of_dataframes.append(df_complete)
                    
                    logger.info(f"Done with fight {counter_2} of {number_of_fights}")
                    counter_2 = counter_2 + 1

                # Merge the dataframes from the report to one single dataframe
                df_weekly = pd.concat(list_of_dataframes, ignore_index = True)  
                df_weekly['reportCode'] = code
                old_codes.append(code)
                save_weekly_data(code, df_weekly)
                
                logger.info(f"Done with code {counter_1} of {number_of_codes}")
                counter_1 = counter_1 + 1
        
        except Exception as e:
            # If an error occurs, this block will execute
            error_message = f"Error processing code '{code}': {e}\n"
            error_details = traceback.format_exc()
            
            # Get the current timestamp
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # Save the error to a separate file.
            # 'a' stands for 'append' so you don't overwrite previous errors.
            with open("error_log.txt", "a") as error_file:
                error_file.write(f"--- Timestamp: {timestamp} ---\n")
                error_file.write(error_message)
                error_file.write(error_details)
                error_file.write("-" * 50 + "\n\n")

            print(f"An error occurred for code '{code}'. The details have been saved to error_log.txt. Continuing to the next code...")
        
        # Saves the codes to the cache file    
        save_cache_codes(old_codes)

        # Appends the data to the parquet dataset.
        append_weekly_data_to_dataset()

        print("Program ran successfully")

if __name__ == "__main__":
    main()