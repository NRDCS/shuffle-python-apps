import socket
import asyncio
import time
import random
import json
import sys

from psycopg2 import connect
from psycopg2 import OperationalError, errorcodes, errors

from walkoff_app_sdk.app_base import AppBase

# need to encode datetime data, since json dump cant handle it
class DatetimeEncoder(json.JSONEncoder):
    def default(self, obj):
        try:
            return super().default(obj)
        except TypeError:
            return str(obj)

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

    def query_postgresql_database(self, username, password, host, port, database_name, query, output_format):
        
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
                row_headers = [x[0] for x in cursor.description]
                json_data = []
                for result in cursor.fetchall():
                    json_data.append(dict(zip(row_headers, result)))
                cursor.close()
                self.db_connection.close()
                if output_format == "JSON":
                    return (json.dumps(json_data, indent=1, cls=DatetimeEncoder))
                else:
                    # csv_keys=json_data[0].keys()
                    csv_data=[]
                    csv_data.append(",".join(row_headers))
                    for row in json_data:
                        line=[]
                        for k in row_headers:
                            value=row.get(k)
                            if value == None:
                                value='-'
                            if "," in str(value):
                                value=f'"{str(value)}"'
                            line.append(str(value))
                        csv_line=",".join(line)
                        csv_data.append(csv_line)

                    return(csv_data)
            except Exception as err:
                err_type, err_obj, traceback = sys.exc_info()
                line_num = traceback.tb_lineno
                return {"Error": f"psycopg2 query ERROR: {err} on line number {line_num}. psycopg2 traceback: {traceback}, type: {err_type}"}

if __name__ == "__main__":
    PostgreSQL.run()
