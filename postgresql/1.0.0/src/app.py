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

    def query_postgresql_database(self, username, password, host, port, database_name, query):
        
        # self.db_connection = self.connection(username, password, host, port, database_name) 
        try:
            self.db_connection = connect(user=username, password= password,
                            host= host,
                            port=port,
                            database= database_name)
            print(f"Connection successful {username}@{database_name} ")                                
        except OperationalError as err:
            conn = None
            return {"Error": f"psycopg2 connection ERROR: {err}"}
        if self.db_connection != None:
            cursor = self.db_connection.cursor()
            try:
                cursor.execute(str(query))
                print("Query executed successfully")
                # res = cursor.fetchall()
                row_headers = [x[0] for x in cursor.description]
                json_data = []
                for result in cursor.fetchall():
                    json_data.append(dict(zip(row_headers, result)))
                cursor.close()
                self.db_connection.close()
                return (json.dumps(json_data, indent=4))  
            except Exception as err:
                err_type, err_obj, traceback = sys.exc_info()
                line_num = traceback.tb_lineno

                return {"Error": f"psycopg2 query ERROR: {err} on line number {line_num}. psycopg2 traceback: {traceback}, type: {err_type}"}

if __name__ == "__main__":
    PostgreSQL.run()
