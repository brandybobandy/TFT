"""ETL Utility Functions

This file conatains utility functions for ETL script readability.

Methods:
--------
    * move_column_inplace - Moves a pd.DataFrame() column to position of choice
    * list_to_sql_values - Converts a python list to str as '{v1, v2, v3 ...}' for insertion into db via psycopg2.cursor.copy_from()
"""

import pandas as pd

def move_column_inplace(df: pd.DataFrame(), col: str, pos: int) ->  pd.DataFrame():
    """Moves pd.DataFrame() column to position of choice as specified

    Parameters
    ----------
    * df: pd.DataFrame()
        A Pandas dataframe object.
    * col: str
        The name of the column to be moved.
    * table: pos
        The position to which the column should be moved.
    """

    col = df.pop(col)
    df.insert(pos, col.name, col)

def list_to_sql_values(alist):
    """Converts a python list to str as '{v1, v2, v3 ...}' for insertion into db via psycopg2.cursor.copy_from()

    Parameters
    ----------
    * alist: list
        A list.
    
    """

    return '{' + ','.join(alist) + '}'