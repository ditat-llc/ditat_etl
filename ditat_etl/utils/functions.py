import os

import pandas as pd 
import numpy as np

# Avoid going to database if files already exists locally.
def load_table(
        tablename: str,
        db_instance=None, 
        force=False,
        query=None,
    ):
    '''
    If using database connection, this function requires
    a ditat_elt.databses.Postgres instance.
    '''
    if os.path.exists(f"{tablename}.csv") and force is False:
        print(f'Loading data {tablename}')
        df = pd.read_csv(f"{tablename}.csv")
    else:
        if not query:
           query = f'SELECT * FROM {tablename};'
        print(f'Going to db to extract {tablename}')
        df = db_instance.query(query, df=True)
        df.to_csv(f"{tablename}.csv", index=False)
    return df




# Split 0 -> number in n uniform splits
def int_to_chunks(number, n):
    l = [int(number / n)] * n
    diff = number % n
    for i in range(diff):
        l[i] += 1
    return l


def sanitize_join_values(values, null_type=None, return_as_list=True):
	'''
	This function aims to format properly the values used to join tables.
	Sometimes you will see values that are used as ids as numeric, also with
	decimals.
	'''
	values = pd.Series(values)

	t = values.dtype

	def f(x):
		if pd.isnull(x):
			return null_type
		try:
			x = float(x)
			x = int(x)
		except:
			pass
		return str(x)

	if t == 'object':
		values = values.apply(f)

	elif values.dtype in ['int64', 'float64']:
		values = values.astype('Int64').astype(str)
		values = values.replace('<NA>', null_type)

	if return_as_list:
		values = values.tolist()

	return values









