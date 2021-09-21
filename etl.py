import time

import pandas as pd

from riotwatcher import TftWatcher

from db import DB, insert_df
from config import get_database_creds, get_api_key
from db_utils import create_match_data_table, create_player_metadata_table, create_player_traits_table, create_player_units_table
from etl_utils import move_column_inplace, list_to_sql_values

# Regions required for API Call's.
my_region1 = 'NA1'
my_region2 = 'AMERICAS'

# Riot API key.
api_key = get_api_key()

# Initialize TftWatcher object that abstracts Riot API requests.
watcher = TftWatcher(api_key=api_key)

# Parameters for API calls.
number_players = 10
number_matches = 10

def get_summonerId(my_region1: str) ->  list:
    '''Gets summoner id's for top n players in Challenger.

    Parameters
    ----------
    * my_region1: str
        Region 'NA1' to be used in API call.

    Returns
    -------
    * list
        A list consisting of player names.   
    '''

    challenger_request = watcher.league.challenger(region=my_region1)
    challenger_data = challenger_request['entries']
    challenger_df = pd.DataFrame(challenger_data)
    top10_summonerId_list = challenger_df.sort_values(by='leaguePoints', ascending=False).head(n = number_players)['summonerId'].tolist()
    
    return top10_summonerId_list

def get_puuid(my_region1: str, summonerId_list: list) ->  list:
    '''Gets summoner puuid's for every summonerID in list returned by get_summonerId.

    Parameters
    ----------
    * my_region1: str
        Region 'NA1' to be used in API call.
    * summonerId_list: list
        A list consisting of player names returned by get_summonerId.

    Returns
    -------
    * list
        A list consisting of player puuid's.   
    '''

    puuid_list = [watcher.summoner.by_id(region = my_region1, encrypted_summoner_id=summonerId)['puuid'] for summonerId in summonerId_list]

    return puuid_list

def get_match_id(my_region2: str, puuid_list) ->  list:
    '''Gets match_id's for every puuid in list returned by get_summoner_puuid.

    Parameters
    ----------
    * my_region2: str
        Region 'AMERICAS' to be used in API call.
    * puuid_list: list
        A list consisting of player puuid's returned by get_summoner_puuid.

    Returns
    -------
    * list
        A list consisting of match_id's.   
    '''

    match_id_request = [watcher.match.by_puuid(region = my_region2, puuid = puuid, count = number_matches) for puuid in puuid_list]
    match_id_list = list(set([match_id for match_ids in match_id_request for match_id in match_ids]))

    return match_id_list

def get_match_data(my_region2:str, match_id_list: list) ->  dict:   
    '''Gets match data for every match_id in list returned by get_summoner_match.

    Parameters
    ----------
    * my_region2: str
        Region 'AMERICAS' to be used in API call.
    * match_id_list: list
        A list consisting of match_id's returned by get_summoner_match.

    Returns
    -------
    * dict
        A dictionary consisting of key value pair match_id: match_data.   
    '''

    match_data_dict = {match_id: watcher.match.by_id(region = my_region2, match_id = match_id) for match_id in match_id_list}
    
    return match_data_dict

def get_match_metadata(match_data: dict) ->  pd.DataFrame():
    '''Gets match metadata for values (match_data) in dict returned by get_match_data.

    Parameters
    ----------
    * match_data: dict
        Match data in json format represented as a dictionary.

    Returns
    -------
    * pd.DataFrame()
        A Pandas DataFrame consisting of match metadata for the match data supplied.   
    '''

    match_datetime = match_data['info']['game_datetime']
    match_length = match_data['info']['game_length']
    game_version = match_data['info']['game_version']
    
    match_metadata = pd.json_normalize(
        data = match_data['metadata']
    )
    
    match_metadata['participants'] = match_metadata['participants'].apply(lambda x: list_to_sql_values([str(y) for y in x]))
    
    match_metadata = match_metadata.assign(
        match_datetime = match_datetime,
        match_length = match_length,
        game_version = game_version 
    )
    
    move_column_inplace(match_metadata, 'match_id', 0)
    move_column_inplace(match_metadata, 'match_datetime', 1)
    move_column_inplace(match_metadata, 'match_length', 2)
    move_column_inplace(match_metadata, 'game_version', 3)

    return match_metadata

def get_match_player_metadata(match_data: pd.DataFrame()) ->  pd.DataFrame():
    '''Gets player metadata for values (match_data) in dict returned by get_match_data.

    Parameters
    ----------
    * match_data: dict
        Match data in json format represented as a dictionary.

    Returns
    -------
    * pd.DataFrame()
        A Pandas DataFrame consisting of player metadata for the match data supplied.   
    '''   

    match_player_metadata = pd.json_normalize(
        data = match_data['info'],
        record_path=['participants']
    ).drop(columns=['traits', 'units'])

    move_column_inplace(match_player_metadata, 'puuid', 0)

    return match_player_metadata

def get_match_player_traits(match_data: pd.DataFrame()) ->  pd.DataFrame():
    '''Gets player traits for values (match_data) in dict returned by get_match_data.

    Parameters
    ----------
    * match_data: dict
        Match data in json format represented as a dictionary.

    Returns
    -------
    * pd.DataFrame()
        A Pandas DataFrame consisting of player traits for the match data supplied.   
    '''   

    match_player_traits = pd.json_normalize(
        data = match_data['info']['participants'],
        record_path=['traits'],
        meta = ['puuid']
    ).drop(columns=['style', 'tier_current', 'tier_total'])

    move_column_inplace(match_player_traits, 'puuid', 0)

    return match_player_traits

def get_match_player_units(match_data: pd.DataFrame()) ->  pd.DataFrame():
    '''Gets player units for values (match_data) in dict returned by get_match_data.

    Parameters
    ----------
    * match_data: dict
        Match data in json format represented as a dictionary.

    Returns
    -------
    * pd.DataFrame()
        A Pandas DataFrame consisting of player units for the match data supplied.   
    '''   

    match_player_units = pd.json_normalize(
        data = match_data['info']['participants'],
        record_path=['units'],
        meta = ['puuid']
    ).drop(columns=['name', 'rarity'])
    match_player_units['items'] = match_player_units['items'].apply(lambda x: list_to_sql_values([str(y) for y in x]))
    move_column_inplace(match_player_units, 'puuid', 0)

    return match_player_units

def pd_to_postgres(df: pd.DataFrame(), table: str):
    '''Uploads a Pandas DataFrame into a PostgreSQL table, creating the table first if it doesn't exist.

    * DB connection opened and managed cursor created.
    * PostgeSQL table created if doesn't already exist.
    * Pandas DataFrame written to PostgreSQL table via insert_df().

    Parameters
    ----------
    * df: pd.DataFrame()
        A Pandas DataFrame to be uploaded into a PostgreSQL table.
    * table: str
        The name of the destination table in PostgreSQL.
    '''   

    with DB(**get_database_creds()).managed_cursor() as cur:
        
        cur.execute(f"select exists(select * from information_schema.tables where table_name='{table}')")
        table_exists = cur.fetchone()[0]
        
        df_exists = len(df) > 0

        table_created = False
        
        if df_exists and not table_exists:
            if (table == 'match_data'):
                cur.execute(create_match_data_table())
                table_created = True
                print(f'-Created {table} table with columns:\n{list(df.columns)}\n')
            elif (table == 'player_metadata'):
                cur.execute(create_player_metadata_table())
                table_created = True
                print(f'-Created {table} table with columns:\n{list(df.columns)}\n')
            elif (table == 'player_units'):
                cur.execute(create_player_units_table())
                table_created = True
                print(f'-Created {table} table with columns:\n{list(df.columns)}\n')
            elif (table == 'player_traits'):
                cur.execute(create_player_traits_table())
                table_created = True
                print(f'-Created {table} table with columns:\n{list(df.columns)}\n')
        
            
        if table_exists or (not table_exists and table_created):
            insert_df(df, cur, table)
        


def run():
    """Sequentially executes the data pipeline.

    1)  get_summonerId()
    2)  get_puuid()
    3)  get_match_id()
    4)  get_match_data
    5a) get_match_metadata()
    5b) get_match_player_metadata()
    5c) get_player_units()
    5d) get_player_units()
    """

    print(f"Beginning ETL script.\n")

    func_start = time.time()
    summonerName_list = get_summonerId(my_region1)
    print(f"get_summonerId runtime: {time.time() - func_start} seconds,")

    func_start = time.time()
    puuid_list = get_puuid(my_region1, summonerName_list)
    print(f"get_puuid runtime: {time.time() - func_start} seconds,")

    func_start = time.time()
    match_list = get_match_id(my_region2, puuid_list)
    print(f"get_match_id runtime: {time.time() - func_start} seconds,")

    func_start = time.time()
    match_data_dict = get_match_data(my_region2, match_list)
    print(f"get_match_data runtime: {time.time() - func_start} seconds\n")

    print(f"Beginning match data extraction / insertion:\n")
    extractions_inserts_start = time.time()
    for match_id, match_data in match_data_dict.items():
        
        print(f'match_id: {match_id}')
        
        match_metadata = get_match_metadata(match_data).astype(str)
        pd_to_postgres(match_metadata, "match_data")
       
        player_metadata = get_match_player_metadata(match_data).astype(str)
        player_metadata['match_id'] = match_id
        move_column_inplace(player_metadata, 'match_id', 1)
        pd_to_postgres(player_metadata, "player_metadata")

        player_units = get_match_player_units(match_data).astype(str)
        player_units['match_id'] = match_id
        move_column_inplace(player_units, 'match_id', 1)
        pd_to_postgres(player_units, "player_units")

        player_traits = get_match_player_traits(match_data).astype(str)
        player_traits['match_id'] = match_id
        move_column_inplace(player_traits, 'match_id', 1)
        pd_to_postgres(player_traits, "player_traits")

        print(f"-Extracted data from Pandas DataFrames and inserted into PostgreSQL tables successfully.\n")
    
    print(f"Extract/Insert runtime: {time.time() - extractions_inserts_start} seconds.\n")

if __name__=='__main__':
    
    progstart = time.time()
    run()
    print(f"Total ETL runtime: {time.time() - progstart} seconds.")
    