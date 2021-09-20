"""Configurations

This file contains methods to return credentials

Methods:
--------
    * get_database_creds - Returns a dictionary with DB credentials.
    * get_api_key - Returns a str with Riot API key.
"""

import os

def get_database_creds() -> dict:
    """Gets DB credentials as a dictionary.

    Returns:
    --------
    * dict
        DB credentials.
    """
    
    return {
        'user': os.environ.get('RIOT_DB_USER'),
        'password': os.environ.get('RIOT_DB_PASSWORD'),
        'db': os.environ.get('RIOT_DB_DBNAME'),
        'host': os.environ.get('RIOT_DB_HOST'),
        'port': int(os.environ.get('RIOT_DB_PORT'))
    }

def get_api_key() -> str:
    """Gets Riot API key as a str.

    Returns:
    --------
    * str
        Riot API key.
    """

    riot_api_key = os.environ.get('RIOT_API_KEY')
    return riot_api_key
