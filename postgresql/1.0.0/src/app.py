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
    app_name = "PostgreSQL"  # this needs to match "name" in api.yaml

    def __init__(self, redis, logger, console_logger=None):
        """
        Each app should have this __init__ to set up Redis and logging.
        :param redis:
        :param logger:
        :param console_logger:
        """
        super().__init__(redis, logger, console_logger)

    def connection(self, username, password, host, port, database_name):
        try:
            conn = connect(user=username, password= password,
                                        host= host,
                                        port=port,
                                        database= database_name)
            print(f"Connection successful, User -->{username} ")                                
        except OperationalError as err:
            print_psycopg2_exception(err)
            conn = None
        return conn  


    def query_postgresql_database(self, username, password, host, port, database_name, query):
        
        self.db_connection = self.connection(username, password, host, port, database_name) 
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
                print_psycopg2_exception(err)

if __name__ == "__main__":
    PostgreSQL.run()
