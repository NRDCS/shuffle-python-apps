import socket
import asyncio
import time
import random
import json
import sys

from psycopg2 import connect
from psycopg2 import OperationalError, errorcodes, errors

from walkoff_app_sdk.app_base import AppBase

class PostgreSQL(AppBase):
    __version__ = "1.0.0"
    app_name = "PostgreSQL"

    def __init__(self, redis, logger, console_logger=None):
        """
        Each app should have this __init__ to set up Redis and logging.
        :param redis:
        :param logger:
        :param console_logger:
        """
        super().__init__(redis, logger, console_logger)

    def query_postgresql_database(self, username, password, host, port, database_name, query):
        
        try:
            conn = connect(user=username, password= password,
                            host= host,
                            port=port,
                            database= database_name)
            print(f"Connection successful {username}@{database_name} ")                                
        except OperationalError as err:
            conn = None
            return {"Error": f"psycopg2 connection ERROR: {err}, pgerror: {err.pgerror}, pgcode:{err.pgcode}"}
        if self.db_connection != None:
            cursor = self.db_connection.cursor()
            try:
                cursor.execute(str(query))
                print("Query executed successfully")
                res = cursor.fetchall()
                cursor.close()
                self.db_connection.close()
                return (json.dumps(res))  
            except Exception as err:
                return {"Error": f"psycopg2 query ERROR: {err}, pgerror: {err.pgerror}, pgcode:{err.pgcode}"}

if __name__ == "__main__":
    PostgreSQL.run()
