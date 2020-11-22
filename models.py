import datetime, pyodbc, sys
import numpy as np, pandas as pd
# Note: this was tested in in a linux environment with a pyodbc library dependent
#       on the ODBC driver provided with FreeTDS
#       configuration files are saved to:
#           /etc/odbcinst.ini
#           /etc/odbc.ini
#           /etc/freetds/freetds.conf
#       See https://gist.github.com/rduplain/1293636 for setup guide

# defines connector class for accessing EDCS sql server:
class EDCS_Connection:
    login_credentials = {}
    connection_str = ''
    connection = None
    cursor = None
    def __init__(self,uid,passwd):
        self.login_credentials = {
            'data_source_name' : 'edcs',
            'database_name'    : 'EDStaff_CET_2020',
            'server_name'      : 'ec2-52-61-2-249.us-gov-west-1.compute.amazonaws.com',
            'port'             : 5433,
            'user'             : {
                'id'     : uid,
                'passwd' : passwd,
            }
        }
        self.connection_str = 'dsn={data_source_name};' \
            'UID={user[id]};' \
            'PWD={user[passwd]};' \
            'DATABASE={database_name};'.format(**self.login_credentials)
        self.connection = pyodbc.connect(self.connection_str)
        self.cursor = self.connection.cursor()
    def fetch_sql(self,sql_str):
        print('_' * 80)
        print('\n< Executing SQL Retreival Script: >\n\n\'{}\'\n\n< Retrieving ... >'.format(sql_str),end='')
        start_time = datetime.datetime.now()
        results = pd.read_sql_query(sql_str,self.connection)
        end_time = datetime.datetime.today()
        retrieval_time = 1000 * (end_time - start_time).total_seconds()
        if retrieval_time > 1000:
            print('\n\n< Retrieved in {:,.3} seconds. >'.format(retrieval_time / 1000))
        else:
            print('\n\n< Retrieved in {:,.1f} milliseconds. >'.format(retrieval_time))
        return results
    def execute_sql(self,sql_str):
        print('\n< Executing SQL Script: >\n\'{}\'\n'.format(sql_str))
        self.cursor = self.cursor.execute(sql_str)
# generic sql object class:
class SQL_Object:
    connection = None
    source = ''
    data = pd.DataFrame()
    fetch_init = None
    def __init__(self):
        pass
    def set_table_cols(self,column_name_list):
        self.data = pd.DataFrame(columns=column_name_list)
    def column_map(self,column_name,modifier_function):
        self.data[column_name] = self.data[column_name].map(modifier_function)
    def rename_column(self,original_column_name,new_column_name):
        self.data = self.data.rename(columns={original_column_name:new_column_name},index={})
    def append_columns(self,data_frame):
        self.data = pd.concat([self.data,data_frame],axis='columns')

# class representing sql table on edcs, extends sql object class:
class EDCS_Table(SQL_Object):
    table_name = ''
    def __init__(self,table_name,uid,passwd,fetch_init=True):
        self.connection = EDCS_Connection(uid,passwd)
        self.source = 'database'
        self.table_name = table_name
        self.fetch_init = fetch_init
        if self.fetch_init:
            self.fetch_table()
    def fetch_table(self):
        sql_str = 'SELECT * FROM {}'.format(self.table_name)
        self.data = self.connection.fetch_sql(sql_str)

# class representing results of a query from edcs, extends sql object class:
class EDCS_Query_Results(SQL_Object):
    sql_str = ''
    def __init__(self,sql_str,uid,passwd,fetch_init=True):
        self.connection = EDCS_Connection(uid,passwd)
        self.source = 'database'
        self.sql_str = sql_str
        self.fetch_init = fetch_init
        if self.fetch_init:
            self.fetch_results()
    def fetch_results(self):
        self.data = self.connection.fetch_sql(self.sql_str)

# class representing a local file:
class Local_CSV:
    source = ''
    file_name = ''
    delimiter = ''
    data = pd.DataFrame()
    filter_csv = None
    filter_function = None
    fetch_init = None
    def __init__(self,file_name,delimiter='|',filter_csv=False,filter_function=None,fetch_init=True):
        self.source = 'csv'
        self.file_name = file_name
        self.delimiter = delimiter
        self.filter_csv = filter_csv
        self.filter_function = filter_function
        self.fetch_init = fetch_init
        if self.fetch_init:
            self.read_file()
    def read_file(self):
        if self.filter_csv:
            print('\n< Loading CSV as iterator: >\n\n{}\n\n< Reading ...      >'.format(self.file_name),end='')
            with open(self.file_name) as f:
                count_rows = np.sum(1 for row in f) - 1
                f.seek(0)
                current_row = 0
                sys.stdout.write('\b' * 6)
                print_str = '{:3.0f}% >'.format(current_row / count_rows * 100)
                print(print_str,end='')
                iter_csv = pd.read_csv(f,delimiter=self.delimiter,iterator=True,chunksize=140)
                filtered_chunks = []
                for chunk in iter_csv:
                    filtered_chunks.extend([self.data, chunk[self.filter_function(chunk)]])
                    current_row = current_row + len(chunk.index)
                    sys.stdout.write('\b' * 6)
                    print_str = '{:3.0f}% >'.format(current_row / count_rows * 100)
                    print(print_str,end='')
                self.data = pd.concat(filtered_chunks)
                print('')
            print('_' * 80)
        else:
            print('\n< Loading CSV: >\n\n{}\n\n< Reading ... >'.format(self.file_name))
            with open(self.file_name) as f:
                self.data = pd.read_csv(f,delimiter=self.delimiter)
            print('_' * 80)
    def set_table_cols(self,column_name_list):
        self.data = pd.DataFrame(columns=column_name_list)
    def column_map(self,column_name,modifier_function):
        self.data[column_name] = self.data[column_name].map(modifier_function)
    def rename_column(self,original_column_name,new_column_name):
        self.data = self.data.rename(columns={original_column_name:new_column_name},index={})
    def append_columns(self,data_frame):
        self.data = pd.concat([self.data,data_frame],axis='columns')
    def write_file(self):
        with open(self.file_name) as f:
            self.data.to_csv(f,sep=self.delimiter,index=False)
