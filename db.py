"""Database Handler

This file contains a class DB to store connection parameters with a managed curser.

Classes:
--------
    * DB - Contains connection parameters for the database as fields, and a managed cursor that opens and closes connections after use automatically.

Methods:
--------
    * managed_cursor - Opens connection to DB, defines cursor, waits for calling function to execute statement, closes cursor, and closes DB connection sequentially.
"""

from contextlib import contextmanager
from dataclasses import dataclass

import io
import csv

import psycopg2
import psycopg2.extras



@dataclass
class DB(object):
    """
    Represents a database connection.

    Attributes
    ----------
    * db: str
        The database name.
    * user: str
        the database user username.
    * password: str
        The database user password
    * host: str
        The database host.
    * port: int
        The database port.

    Methods
    -------
    * managed_cursor(self, cursor_factory=None)
        Opens connection to DB, defines cursor, waits for calling function to execute statement, closes cursor, and closes DB connection sequentially.
    """

    db: str
    user: str
    password: str
    host: str
    port: int = 5432

    @contextmanager
    def managed_cursor(self, cursor_factory=None):
        """Opens connection to DB, defines cursor, waits for calling function to execute statement, closes cursor, and closes DB connection sequentially.

        Parameters
        ----------
        * self: object
            DB class defines all necessary parameters upon creation.
        * cursor_factory: object
            A subclass of the generic psycopg2 cursor found in psycopg2.extras providing a different interface for execuiting queries.
        
        Returns
        -------
        * Cursor object
            A psycopg2 Cursor object from which SQL statements can be executed.
        """

        self.conn_url = (f'postgresql://{self.user}:{self.password}@{self.host}:{self.port}/{self.db}')
        self.conn = psycopg2.connect(self.conn_url)
        self.conn.autocommit = True
        self.curr = self.conn.cursor(cursor_factory=cursor_factory)
        try:
            yield self.curr
        finally:
            self.curr.close()
            self.conn.close()

def insert_df(df, cur, table):
    """Inserts a pd.DataFrame() object into the DB table.

    * Managed cursor with DB connection provided.
    * Pandas DataFrame written to temporary csv with StringIO.
    * Temporary csv written to temporary PostgreSQL table.
    * Rows from temporary PostgreSQL table written to destination table passing over those that violate unique constraint.
    * Temporary PostgreSQL table deleted.

    Parameters
    ----------
    * df: pd.DataFrame()
        A Pandas dataframe object.
    * cur: psycopg2.connect.cursor()
        A psycopg2 Cursor object.
    * table: str
        The name of the destination table in DB.
    """

    df_columns = list(df)

    string_buffer = io.StringIO()
    df.to_csv(string_buffer, index=False, header=False, sep='|')
    string_buffer.seek(0)

    tmp_table = "tmp_table"

    cur.execute(
        f"""
         CREATE TEMP TABLE {tmp_table}
         AS
         SELECT * 
         FROM {table}
         WITH NO DATA
         """
    )

    cur.copy_from(file=string_buffer, table=tmp_table, sep='|', null="", columns=df_columns)

    cur.execute(
        f"""
         INSERT INTO {table}
         SELECT *
         FROM {tmp_table}
         ON CONFLICT DO NOTHING
         """
    )

    cur.execute(
        f"""
         DROP TABLE {tmp_table}
         """
    )




