import json
from functools import wraps
from ast import literal_eval

import psycopg2
from psycopg2.extras import RealDictCursor
import pandas as pd

from ..time import TimeIt


class Postgres:
    TYPES_MAPPING = {
        'text': str,
        'bigint': int,
        'double precision': float,
        'timestamp without time zone': 'datetime64',
        'timestamp with time zone': 'datetime64',
        'integer': int,
        'character varying': str,
        'boolean': bool,
        'real': float,
        'uuid': str,
        'numeric': float,
        'date': 'datetime64',
        'jsonb': dict,
        'ARRAY': list
    }
    SF_TO_POSTGRES_TYPES = {
        'anytype': 'varchar',
        'base64': 'varchar',
        'boolean': 'boolean',
        'combobox': 'varchar',
        'currency': 'numeric',
        'datacategorygroupreference': 'varchar',
        'date': 'timestamp',
        'datetime': 'timestamp',
        'double': 'numeric',
        'email': 'varchar',
        'encryptedstring': 'varchar',
        'id': 'varchar',
        'int': 'integer',
        'multipicklist': 'varchar',
        'percent': 'numeric',
        'phone': 'varchar',
        'picklist': 'varchar',
        'reference': 'varchar',
        'string': 'varchar',
        'textarea': 'varchar',
        'time': 'time',
        'url': 'varchar',
        'address': 'jsonb'
    }

    def __init__(self, config, keep_connection_alive=False):
        '''
        Args:
            config (dict): Client configuration
            {
                "database": "xxxx",
                "user": "xxxx",
                "password": "xxxx",
                "host": "xxxxx",
                "port": "xxxx"
            }

            keep_connection_alive (bool, default):
                Persist the connection as an attribute of the class or create
                and close the connection with each call. Similar to a context
                manager.

        '''
        self.config = config
        self._schema = 'public'
        self.keep_connection_alive = keep_connection_alive

        if self.keep_connection_alive is True:
            self.connect()

    def connect(self):
        '''
        Only triggered when self.keep_connection_alive == True.
        '''
        self.conn = psycopg2.connect(**self.config)

    def close_conn(self):
        self.conn.close()

    def table_exists(function):
        '''
        Decorator to check if table exists.
        '''
        @wraps(function)
        def wrapper(self, tablename, *args, **kwargs):
            if tablename not in self.tables:
                print(f'Table {tablename} does not exist for {self._schema} schema!')
                return None
            result = function(self, tablename, *args, **kwargs)
            return result
        return wrapper

    @property
    def schema(self):
        return self._schema

    @schema.setter
    def schema(self, value):
        self._schema = value

    @TimeIt()
    def query(
        self,
        query_statement: list or str,
        df: bool=False,
        as_dict: bool=False,
        commit: bool=True,
        returning: bool=True,
        mogrify: bool=False,
        mogrify_tuple: tuple or list=None,
        verbose=False
    ):
        '''
        Low level method for querying.

        Notes:
            - If self.keep_connection_alive is False,
                this method will create and destroy the connection each time.
                If it is True, it will persist the connection.

        Args:
            - query_statement (str or list): A query or list of queries.
                Order is important.
            
            - df (boolean, default=False): results as list of pd.DataFrame.
            
            - as_dict (boolean, default=False): If df is False, you have the
                option of returning the column names in a dict instead of a list.

            - commit (bool, default=True).

            - returning (bool, default=True): Does the query or list of queries return.
                Only returns the last query. Order is important.

            - mogrify (bool, default=False): Mogrify the query passing a tuple for a single.

            - mogrify_tuple (iterable, default=None): Values to be used when mogrigy is True.

            - verbose (bool, default=True): Print query.

        Returns
            - results (list or pd.DataFrame or dict): Depending on the df
                parameter and also the parameter.
                If given more than one query, it will only return the result
                of the last one. Order of queries is important.
        '''
        conn = psycopg2.connect(**self.config) \
            if not self.keep_connection_alive else self.conn

        conn.autocommit = True if commit else False
    
        try:
            query_statement_lst = query_statement \
                if isinstance(query_statement, list) else [query_statement]
                    
            mogrify_tuple_list = mogrify_tuple \
                if isinstance(query_statement, list) else [mogrify_tuple]

            cursor = conn.cursor(cursor_factory=RealDictCursor) if as_dict else conn.cursor()  
            
            for index, query_statement in enumerate(query_statement_lst):

                if mogrify:
                    query_statement = cursor.mogrify(
                        query_statement,
                        mogrify_tuple_list[index]
                    )

                if verbose:
                    print(query_statement)

                if returning and index == (len(query_statement_lst) - 1):
                    # Fetching only the last query
                    if df:
                        results = pd.read_sql(query_statement, conn)
                    
                    else:
                        cursor.execute(query_statement)
                        results = cursor.fetchall()

                        if as_dict:
                            results = [dict(i) for i in results]
                else:
                    cursor.execute(query_statement)
                    results = cursor.statusmessage

            cursor.close()
            return results
        
        except Exception as e:
            print(e)

        finally:
            if self.keep_connection_alive is False:
                conn.close()        

    @property
    def tables(self):
        '''
        Returns list of tables for the current schema.
        Value changes if given another schema
        '''
        query = 'SELECT table_name FROM information_schema.tables WHERE table_schema = %s;'
        result = self.query(
            query_statement=query,
            commit=False,
            mogrify=True,
            mogrify_tuple=(self.schema,)
        )
        result = [i[0] for i in result]
        result.sort()
        return result

    @table_exists
    def get_table_info(self, tablename):
        '''
        Returns all the column names of the table

        Notes:
            - Assuming the schema given. Would have to set schema to a different value if needed.

        Returns:
            result (list): List of column names for tablename
        '''
        query = 'SELECT * FROM information_schema.columns WHERE table_schema = %s AND table_name = %s;'

        result = self.query(
            query_statement=query,
            df=True,
            commit=False,
            mogrify=True,
            mogrify_tuple=(self.schema, tablename)
        )
        return result

    @table_exists
    def get_table_cols(self, tablename, sort=False):
        '''
        Returns all the column names of the table

        Notes:
            - Assuming the schema given. Would have to set schema to a different value if needed.

        Returns:
            - result (list): List of column names for tablename
        '''
        table_info = self.get_table_info(tablename)
        columns = table_info.column_name
        if sort:
            columns = columns.sort_values()
        return columns.tolist()

    @table_exists
    def get_table_data_types(self, tablename, df=False, sql_types=False):
        '''
        Returns a dictionary with column_name: data type in python mapping
        given the tablename.

        Args:
            tablename (str): Table name for the self._schema assigned.

        Returns:
            result (dict)            
        '''
        table_info = self.get_table_info(tablename)

        if sql_types is False:

            table_info['data_type'] = table_info['data_type'].map(type(self).TYPES_MAPPING) 

        result = table_info[['column_name', 'data_type']]

        if df is False:

            result = [
                value for index, value in result.set_index('column_name').to_dict().items()
            ][0]

        return result 

    @TimeIt()
    def insert_df_to_sql(
        self,
        df: pd.DataFrame,
        tablename: str,
        commit=True,
        conflict_on: str or list=None,
        do_update_columns: bool or list=False,
        verbose=False
    ):
        '''
        Replacement for pd.to_sql() since it drops and inserts the whole table.

        Args:
            - df (pd.DataFrame): Values to be updated.

            - tablename (str): Table name to in the db.

            - commit (bool, default=True): Commit changes.

            - conflict_on (str or list, default=None): Primary key(s) or unique
                indexes.

            - do_update_columns(bool or list, default=False):
                if False: DO NOTHING.
                if True: updates all the non-index/ constraint columns
                if list: updates only columns in list.

                ! BEWARE: This overwrites all columns, even if the given values
                    are null.

            - verbose (bool, default=False): Print query.

        Returns:

            - 'INSERT 0 {N_RECORDS}'
        '''
        df = df.copy()

        # Casting correct data types
        table_data_types = self.get_table_data_types(tablename)

        filtered_data_types = {
            k: j for k, j in table_data_types.items() if k in df.columns
        }

        for col in df.columns:
            data_type = filtered_data_types[col]

            if data_type == list:
                try:
                    df[col] = df[col].apply(literal_eval)
                except:
                    pass

            elif data_type == dict:

                try:
                    df[col] = df[col].apply(literal_eval).apply(json.dumps)
                except:
                    # review this change, Portuguese and weird encodings
                    pass

            elif data_type in [int, float]:
                # Workaround: Cannot place None with numerical.
                df[col] = df[col].astype(str)
                df[col] = df[col].replace({'nan': None})

                ### under evaluation
                df[col] = df[col].replace({'None': None})

                if data_type == int:
                    df[col] = df[col].apply(lambda x: x.split('.')[0] if x is not None else x)

            elif data_type == str:
                if df[col].dtype != 'object':
                    df[col] = df[col].astype(str)
                    df = df.replace({'nan': None})

        df = df.where(pd.notnull(df), None)
        ######

        df_dict = df.to_dict(orient='records')
        keys = ', '.join(df_dict[0].keys())
        values = [tuple(i.values()) for i in df_dict]

        query = '''INSERT INTO {} ({}) VALUES '''.format(tablename, keys)
        query += ', '.join(["%s"] * len(values))

        if conflict_on:

            conflict_on = conflict_on if isinstance(conflict_on, list) else [conflict_on]

            if do_update_columns is False:
                query += f" ON CONFLICT ({', '.join(conflict_on)}) DO NOTHING"

            else:
                query += f" ON CONFLICT ({', '.join(conflict_on)}) DO UPDATE SET "

                table_columns = do_update_columns if isinstance(do_update_columns, list) else self.get_table_cols(tablename)
                # add checker of columns in table
                table_columns = list(set(table_columns) - set(conflict_on))

                table_columns = ', '.join([f"{i} = EXCLUDED.{i}" for i in table_columns])
                query += table_columns

        query += ";"

        result = self.query(
            query_statement=query,
            df=False,
            as_dict=False,
            commit=commit,
            returning=False,
            mogrify=True,
            mogrify_tuple=values,
            verbose=verbose
        )
        print(result)
        return result

    @TimeIt()
    def update_df_to_sql(
        self,
        df: pd.DataFrame,
        tablename: str,
        on_columns: str or list,
        insert_new=True,
        commit=True,
        verbose=False,
        overwrite=False
        ):
        '''
        This implementation is slightly different from
        self.insert_df_to_sql() (which really upserts),
        because we need to update values without necessarily having
        a constraint such as an index/primary key.

        This method also gives you the ability to "upsert" without having
        a constraint like a primary key or index(es)

        Args:
            - df (pd.DataFrame): Values to be updated.

            - tablename (str): Table name to in the db.

            - on_columns (str or list): which column(s) to use as the identity
                for updating the values. Serves as a "primary key".

            - insert_new (bool, default=True): Insert values that are not already
                present in the table according to "on_columns".

            - commit (bool, default=True): Commit changes.

            - verbose (bool, default=False): Print query.

            - overwrite (bool, default=False): If False, values will only be updated if
                existing evaluate to NULL.

        Returns:
            - {'update': 'UPDATE {N_RECORDS}'} or {'update': 'UPDATE {N_RECORDS}', 'INSERT 0 {N_RECORDS}'}
        '''
        df = df.copy()

        table_data_types = self.get_table_data_types(tablename)
        # following line used for coalesce and casting
        table_raw_data_types = {
            i: (j if j not in ['ARRAY'] else 'varchar[]') for i, j in self.get_table_data_types(tablename, sql_types=True).items()
        }
        table_columns = list(table_data_types.keys())

        for col in df.columns:
            if col not in table_columns:
                raise ValueError(f'{col} does not exist in the table {tablename}.')
        
        on_columns = on_columns if isinstance(on_columns, list) else [on_columns]
        updated_columns = [col for col in df.columns if col in table_columns if col not in on_columns]

        df = df[on_columns + updated_columns]

        # Data formatting
        filtered_data_types = {k: j for k, j in table_data_types.items() if k in df.columns}

        for col in df.columns:
            data_type = filtered_data_types[col]

            if data_type == list:
                df[col] = df[col].apply(literal_eval)

            elif data_type == dict:
                df[col] = df[col].apply(literal_eval).apply(json.dumps)

            elif data_type in [int, float]:
                # Workaround: Cannot place None with numerical.
                df[col] = df[col].astype(str)
                df[col] = df[col].replace({'nan': None})

                if data_type == int:
                    df[col] = df[col].apply(lambda x: x.split('.')[0] if x is not None else x)

            elif data_type == str:
                if df[col].dtype != 'object':
                    df[col] = df[col].astype(str)
                    df = df.replace({'nan': None})

        df = df.where(pd.notnull(df), None)
        ###

        records = df.to_dict(orient='records')
        records = [tuple(value.values()) for value in records]

        if overwrite:
            initial_source = 'df'
            secondary_source = 'target'
        else:
            initial_source = 'target'
            secondary_source = 'df'

        query = '''UPDATE {} as target SET {} FROM (VALUES {}) AS df({}) WHERE {};'''.format(
            tablename,

            ', '.join([f"{col} = COALESCE({initial_source}.{col}::{data_type}, \
                {secondary_source}.{col}::{data_type})" for col, \
                data_type in table_raw_data_types.items() if col in updated_columns]),

            ', '.join(["%s"] * len(records)),

            ', '.join(df.columns.tolist()),

            'AND '.join([f"df.{col} = target.{col}" for col in on_columns])
        )
        results = {}

        result = self.query(
            query_statement=query,
            df=False,
            as_dict=False,
            commit=commit,
            returning=False,
            mogrify=True,
            mogrify_tuple=records,
            verbose=verbose
        )
        results['update'] = result

        if insert_new:
            on_columns_fmt = ', '.join(on_columns)
            existing_query = 'SELECT {} FROM {}'.format(
                on_columns_fmt,
                tablename
            )
            existing_df = self.query(
                query_statement=existing_query,
                df=True
            )
            new_df = pd.merge(
                left=df,
                right=existing_df,
                how='left',
                on=on_columns,
                suffixes=('', '_y'),
                indicator=True
            )
            new_df = new_df[new_df['_merge'] == 'left_only']
            new_df = new_df[on_columns + updated_columns]

            if new_df.shape[0] > 0:
                result = self.insert_df_to_sql(
                    df=new_df,
                    tablename=tablename,
                    commit=commit,
                    conflict_on=None,
                    do_update_columns=False,
                    verbose=verbose
                )
                results['insert'] = result
            else:
                results['insert'] = 'INSERT 0 0'

        print(results)
        return results

    def create_table(
        self,
        tablename,
        column_mappings,
        primary_key='id',
        if_not_exists=True,
        commit=True
        ):
        '''
        Create table in DB from source;

        Args:
            - tablename (str) 

            - column_mappings (dict): Dictionary of names and postgres datatypes

            - primary_key (str or False): Create an index on a single column
                ** Does not support multiple-column index. You need to call self.create_table_index()

            - if_not_exists(bool, default=True)

            - commit(bool, default=True)
        '''
        query = 'CREATE TABLE '

        if if_not_exists:
            query += 'IF NOT EXISTS '

        query += f"{tablename} "

        columns = []

        for column in column_mappings:
            s = f"{column['name']} {column['type']}"
            
            if primary_key and column['name'] == primary_key:
                s += ' PRIMARY KEY'
            columns.append(s)

        columns_fmt = '(' + ', '.join(columns) + ')'

        query += columns_fmt
        query += ';'

        self.query(
            query_statement=query,
            commit=commit,
            returning=False,
            verbose=True
        )

    def create_table_index(
        self,
        tablename: str,
        index_name: str,
        columns: str or list,
        unique=False,
        method='btree',
        ):
        '''
        * We don't use the @table_exists because it is used when we check columns
        '''
        columns = columns if isinstance(columns, list) else [columns]
        
        table_columns = self.get_table_cols(tablename)

        if not all(elem in table_columns  for elem in columns):
            raise AssertionError(f"One or more columns provided don't exist in table {tablename}.")

        columns_fmt = ', '.join(columns)

        query = "CREATE"

        if unique:
            query += ' UNIQUE'

        query += f' INDEX {index_name} ON {tablename} USING {method}({columns_fmt});'

        self.query(
            query_statement=query,
            commit=True,
            verbose=True
        )
