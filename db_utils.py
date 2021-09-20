"""DB Utility Functions

This file contains get() methods to retrieve the SQL query of choice in str format.

Methods: 
--------
    * create_match_data_table - Returns sql text to create table match_data.
    * create_player_metadata_table - Returns sql text to create player_metadata.
    * create_player_units_table - Returns sql text to create table player_units.
    * create_player_traits_table - Returns sql text to create table player_units.
"""

def create_match_data_table() :
    """Return SQL statement to create match_data table in DB.

    Returns
    -------
    * str
        An SQL CREATE TABLE statement.
    """

    query = """
            CREATE TABLE match_data (
                match_id VARCHAR(255) PRIMARY KEY,
                match_datetime VARCHAR(255),
                match_length VARCHAR(255),
                game_version VARCHAR(255),
                data_version VARCHAR(255),
                participants VARCHAR(255)[],
                timestamp timestamp default current_timestamp
            )
            """
    return query

def create_player_metadata_table():
    """Return SQL statement to create player_metadata table in DB.

    Returns
    -------
    * str
        An SQL CREATE TABLE statement.
    """
    
    query = '''
            CREATE TABLE player_metadata (
                puuid VARCHAR(255),
                match_id VARCHAR(255),
                gold_left VARCHAR(255),
                last_round VARCHAR(255),
                level VARCHAR(255),
                placement VARCHAR(255),
                players_eliminated VARCHAR(255),
                time_eliminated VARCHAR(255),
                total_damage_to_players VARCHAR(255),
                "companion.content_ID" VARCHAR(255),
                "companion.skin_ID" VARCHAR(255),
                "companion.species" VARCHAR(255),
                PRIMARY KEY (puuid, match_id),
                timestamp timestamp default current_timestamp
            )
            '''
    return query

def create_player_units_table():
    """Return SQL statement to create player_units table in DB.

    Returns
    -------
    * str
        An SQL CREATE TABLE statement.
    """
    
    query = """
            CREATE TABLE player_units (
                puuid VARCHAR(255),
                match_id VARCHAR(255),
                character_id VARCHAR(255),
                items VARCHAR(255)[],
                tier VARCHAR(255),
                PRIMARY KEY (puuid, match_id),
                timestamp timestamp default current_timestamp
            )
            """
    return query

def create_player_traits_table():
    """Return SQL statement to create player_traits table in DB.

    Returns
    -------
    * str
        An SQL CREATE TABLE statement.
    """
    
    query = """
            CREATE TABLE player_traits(
                puuid VARCHAR(255),
                match_id VARCHAR(255),
                name VARCHAR(255),
                num_units VARCHAR(255),
                PRIMARY KEY (puuid, match_id),
                timestamp timestamp default current_timestamp
            )
            """
    return query