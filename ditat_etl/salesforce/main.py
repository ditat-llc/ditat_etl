import os
import json
import inspect
from datetime import timedelta, datetime
import requests
from concurrent.futures import ThreadPoolExecutor
from typing import List

import pandas as pd
import numpy as np
from simple_salesforce import Salesforce

pd.options.mode.chained_assignment = None


from ..time import TimeIt


filedir = os.path.abspath(os.path.dirname(__file__))


class SalesforceObj():
	
	TYPES_MAPPING = {
		'string': str,
		'reference': str,
		'datetime': 'datetime',
		'picklist': str,
		'boolean': bool,
		'date': 'date',
		'double': float,
		'phone': str,
		'url': str,
		'email': str,
		'id': str,
		'textarea': str,
		'int': int,
		'address': str,
		'multipicklist': list,
		'currency': float,
		'percent': float
	}

	MAX_QUERY_SIZE = 100

	def __init__(
		self,
		config,
		client_id=None,
		client_secret=None,
		):
		req_params =  inspect.getargspec(Salesforce)[0]
		self.config_params = {i: j for i, j in config.items() if i in req_params}
		self.config_params['version'] = '53.0'

		self.client_id = client_id
		self.client_secret = client_secret
		self.load_client_credentials()
		
		self.access_token = None
		self.instance_url = None
		self.login()

	def load_client_credentials(self):
		if not self.client_id or not self.client_secret:

			with open(os.path.join(filedir, 'credentials.json'), 'r') as f:
				file = f.read()

			try:
				credentials = json.loads(file)
				self.client_id = credentials.get('CLIENT_ID')
				self.client_secret = credentials.get('CLIENT_SECRET')

			except Exception:
				print('Could not load SF client credentials.')

	def __str__(self):
		return self.sf.sf_instance

	def login(self):
		'''
		This class will eventually only use oauth2 refresh tokens because
		starting in Feb 2022, all username/password login will require MFAs

		'''
		refresh_token = self.config_params.get('session_id') 

		if refresh_token:
			self.refresh_token(refresh_token)
			self.sf = Salesforce(instance_url=self.instance_url, session_id=self.access_token)
			print('Salesforce login using refresh token.')

		else:
			self.sf = Salesforce(**self.config_params)
			print('Salesforce login using username/password.')

	def refresh_token(
		self,
		refresh_token: str,
		client_id: str=None,
		client_secret: str=None,
		url="https://login.salesforce.com/services/oauth2/token",
		):
		'''
		Refresh access token for login.

		Args:
			- refresh_token (str)

			- client_id (str, default=None)

			- client_secret (str, default=None)

			- url (str, default='https://login.salesforce.com/services/oauth2/token')
			
		'''
		payload = {
			'grant_type': 'refresh_token',
			'client_id': client_id or self.client_id,
			'client_secret': client_secret or self.client_secret,
			'refresh_token': refresh_token
		}
		resp = requests.post(url=url, data=payload).json()

		self.access_token = resp['access_token']
		self.instance_url = resp['instance_url']
			
	def check_table_exists(
		self,
		tablename,
		verbose=True
		):
		'''
		Custom sobjects/tables are not included in the property attribute,
		hence we use this wrapper to see if the sobject exits for the client.
		
		Args:
			tablename (str): name of the sobject.

			verbose (bool, default=False): print the possible error.
		'''
		try:
			query = '''
				SELECT
					COUNT(Id)
				FROM
					{}
			'''.format(tablename)
			count = self.sf.query(query).get('records')[0].get('expr0')

			# if count == 0:
			# 	return None

			return True

		except Exception as e:
			if verbose:
				print(e)

			return None

	@property
	def tables(self):
		'''
		Get all the tables/SObjects
		'''
		return [obj['name'] for obj in self.sf.describe()['sobjects']]

	def get_table_info(self, tablename, columns=None):
		if not self.check_table_exists(tablename):
			return None

		table = getattr(self.sf, tablename)
		df = pd.DataFrame(table.describe()['fields'])

		if columns is not None and all(item in df['name'].tolist() for item in columns):
			df = df[columns]

		return df

	def map_types(
		self,
		df,
		tablename,
		check_column_casing=True,
		return_as_dict=False
	):
		'''
		Preparation of dataframe to be Salesforce compatible.

		Args:
			- df (pd.DataFrame)

			- tablename (str): Sobject in Salesforce.

			- check_column_casing (bool, default=True): Manage casing for column
				names.

			- return_as_dict (bool, default=False): Return as dataframe or dict.

		Returns:
				- df or data_list 

		'''
		df = df.copy()
		
		info = self.get_table_info(tablename)[['name', 'type']]

		if check_column_casing:
			info['lower_name'] = info['name'].str.lower()

			df.columns = [c.lower() for c in df.columns]

			case_mapping = {
				i: j for i, j in info.set_index('lower_name')['name'].to_dict().items() \
				if i in df.columns
			} 
			df.rename(columns=case_mapping, inplace=True)

		info = info.loc[info.name.isin(df.columns)]

		info['python_type'] = info['type'].map(type(self).TYPES_MAPPING)
		info.loc[info['python_type'].isnull(), 'python_type'] = str

		mapping = info.set_index('name')['python_type'].to_dict()
		mapping = {i: j for i, j in mapping.items() if j not in [list, dict]}

		df = df[[col for col in df.columns if col in mapping.keys()]]

		for k, v in mapping.items():

			if v in [list, dict]:
				continue

			elif v == "date":
				df[k] = df[k].astype('datetime64[ns]').dt.strftime("%Y-%m-%d")

				if return_as_dict:
					df[k] = df[k].fillna(-99_999_999)

			elif v == 'datetime':
				df[k] = df[k].astype('datetime64[ns]').dt.strftime('%Y-%m-%dT00:00:00.000Z')

				if return_as_dict:
					df[k] = df[k].fillna(-99_999_999)

			else:
				df[k] = df[k].astype(v, errors='ignore')

			# Trick to handle null value for numeric when returning dict
			if v in [int, float] and return_as_dict is True:
				df[k] = df[k].fillna(-99_999_999)

		df = df.where(pd.notnull(df), None)

		if return_as_dict is False:
			return df

		# Returning as dict handling nan values
		data = df.to_dict(orient='records')

		data_list = []

		for d in data:
			new = {
				i: (None if j in [
					pd.NaT,
					np.nan,
					'nan',
					'None',
					-99_999_999
				] else j) for i, j in d.items()
			}

			data_list.append(new)

		return data_list
		
	def get_table_cols(
		self,
		tablename
		):
		'''
		Returns all column names of table / SObject of self.sf

		Args:
			tablename (str)

		Returns
			Column names	
		'''
		return self.get_table_info(tablename, columns=['name'])['name'].tolist()

	# @TimeIt()
	def query(
		self,
		tablename,
		columns=None,
		limit=None,
		df=False,
		date_from=None,
		date_to=None,
		date_window=None,
		date_window_variable='LastModifiedDate',
		verbose=False,
		include_deleted=True,
		timeout=None,
		max_columns=100, # SF limit on columns
		):
		'''
		Returns dictionary of the columns requested from the table / SObject specified.
		If no columns or wrong columns are provided, it returns all the columns.

		Args:
			tablename (str): SObject Resource from self.sf
			
			columns (str: default=None): Columns names to be queried.

			limit (int): CHECK THIS! You have to add a limit.

			df(boolean default: False): Output format Dict or pd.DataFrame
			
			date_from (str, default=None): It has to be in the correct format.

			date_to (str, default=None): It has to be in the correct format.
			 
			date_window (int, default=None): How many days to go back

			date_window_variable (str, default='LastModifiedDate'):
				Column name to be used for date window
			 
			verbose (bool, default=False): print the query

			include_deleted (bool, default=True): Include deleted records

			timeout (int, default=None): Timeout for the query

			max_columns (int, default=100): Maximum number of columns to be queried

		Returns:
			Output format Dict or pd.DataFrame from query

		Notes:
			- You have to decide between date_window or (date_to and date_from)
				There is no validation for this observation.

		'''
		####
		def chunker(seq, size):
			return (seq[pos:pos + size] for pos in range(0, len(seq), size))
		#####

		if not hasattr(self.sf, tablename):
			return None

		column_names = self.get_table_cols(tablename)

		if columns is None or not all(item in column_names for item in columns):
			columns = column_names

		# Splitting in chunks
		chunks = chunker(columns, max_columns - 1)

		counter = 0
		for chunk in chunks:

			if 'Id' not in chunk:
				chunk = ['Id'] + chunk
			query_cols = ', '.join(chunk)
			query = '''
				SELECT
					{}
				FROM
					{}
			'''.format(
				query_cols,
				tablename
			)
			# Using date_window
			if isinstance(date_window, int) and date_window > 0:
				date_from = (
					datetime.today() - timedelta(days=date_window)
				).strftime('%Y-%m-%dT00:00:00.000Z')

				query += f" WHERE {date_window_variable} >= {date_from}"

			# Using date_to AND date_from
			elif date_from and date_to:
				query += f" WHERE {date_window_variable} >= {date_from} AND {date_window_variable} <= {date_to}"

			elif date_from:
				query += f" WHERE {date_window_variable} >= {date_from}"

			elif date_to:
				query += f" WHERE {date_window_variable} <= {date_to}"

			if isinstance(limit, int):
				query += f' LIMIT {limit}'

			if verbose:
				print(query)

			results = self.sf.query_all(
				query,
				include_deleted=include_deleted,
				timeout=timeout
			)['records'] 

			# Only easy way to marge chunks on Id through a DataFrame
			results = pd.DataFrame.from_dict(results)

			if results.shape[0] < 1:
				return None

			if 'attributes' in results.columns:
				results.drop('attributes', axis=1, inplace=True)
			else:
				for r in results:
					r.pop('attributes', None)

			if counter == 0:
				all_results = results
			
			else:
				all_results = pd.merge(all_results, results, on='Id', how='left')

			counter += 1			

		if df == False:
			all_results = all_results.to_dict(orient='records')

		return all_results

	############ PARALLEL QUERY ##########
	@TimeIt()
	def query_parallel(
		self,
		tablename,
		columns=None,
		limit=None,
		df=False,
		date_window=None,
		date_window_variable='LastModifiedDate',
		verbose=False,
		include_deleted=True,
		n_chunks=4 # Unfortunately SF limits this to 10
	):
		###### 
		def limit_split(limit, n):
			if limit is None:
				return [None] * n
			l = [int(limit / n)] * n
			diff = limit % n
			for i in range(diff):
				l[i] += 1
			return l
		############

		if date_window:
			date_from = (datetime.today() - timedelta(days=date_window))
		
		else:
			# If no date_window is provided, we retrive the oldest date according to "CreatedDate"
			date_window_variable = 'CreatedDate'

			min_date_query = f"SELECT MIN({date_window_variable}) FROM {tablename}"

			date_from_fmt = self.sf.query(
				min_date_query, include_deleted=include_deleted
			)['records'][0]['expr0']

			date_from_fmt = date_from_fmt or '2000-01-01T00:00:00.000Z'

			date_from = datetime.strptime(date_from_fmt, '%Y-%m-%dT%H:%M:%S.000+0000')
			# if date_from_fmt:
			# 	date_from = datetime.strptime(date_from_fmt, '%Y-%m-%dT%H:%M:%S.000+0000')
			#
			# else:
			# 	date_from = datetime.strptime("1970-01-01T00:00:00.000+0000", '%Y-%m-%dT%H:%M:%S.000+0000')

		date_from_fmt = date_from.strftime('%Y-%m-%dT00:00:00.000Z')

		date_to = datetime.today() + timedelta(days=2)

		diff = (date_to  - date_from ) / n_chunks

		chunks = []

		for i in range(n_chunks):
			start = (date_from + diff * i).strftime('%Y-%m-%dT00:00:00.000Z')
			end = (date_from + diff * (i + 1)).strftime('%Y-%m-%dT00:00:00.000Z')
			chunks.append((start, end))
		
		chunks = [c for c in chunks if c[0] != c[1]]

		payload = {
			'tablename': [tablename] * len(chunks),
			'columns': [columns] * len(chunks),
			'limit': limit_split(limit, n_chunks),
			'df': [df] * len(chunks),
			'date_from': [i[0] for i in chunks],
			'date_to': [i[1] for i in chunks],
			'data_window': [date_window_variable] * len(chunks),
			'data_window_variable': [date_window_variable] * len(chunks),
			'verbose': [verbose] * len(chunks),
			'include_deleted': [include_deleted] * len(chunks)
		}

		with ThreadPoolExecutor() as executor:
			results = executor.map(self.query, *payload.values())
		results = [*results]

		if len([i for i in results if i is not None]) == 0:
			return pd.DataFrame()
		
		if df:
			results = pd.concat(results).reset_index(drop=True)

		else:
			results = [item for sublist in results for item in sublist]
		
		return results
	######################################

	@staticmethod
	def chunker(seq, size):
		return (seq[pos:pos + size] for pos in range(0, len(seq), size))

	def update(
		self,
		tablename,
		record_list,
		batch_size=10000,
		return_result=False
		):
		'''
		Args:
			tablename (str): name of the sobject.
			record_list (list or np.array or pd.Series): List
				of records for the update
			batch_size (int, default=100000): After 10_000, it uses multithreading.

			format: "[{'Id': "11111", 'field1': '1111'}]"
		'''
		if not self.check_table_exists(tablename):
			return None

		bulk_handler = getattr(self.sf, "bulk")
		bulk_object = getattr(bulk_handler,  tablename)

		result = bulk_object.update(data=record_list, batch_size=batch_size)

		success = 0
		failure = len(result)

		for r in result:
			if r.get('success') == True:
				success += 1
				failure -= 1

		response = {"success": success, "failure": failure}

		if return_result:
			response['result'] = result

		return response

	def upsert_df_old(
		self,
		tablename,
		dataframe: pd.DataFrame,
		batch_size: int=10_000,
		update: bool=True,
		insert: bool=True,
		conflict_on: str or List[str]='Id',
		return_response: bool=False,
		verbose=False
	):
		'''
		Args:
			- tablename (str): SObject in Salesforce

			- dataframe (pd.DataFrame): Data dataframe.

			- batch_size (int, default=10_000): Salesforce default.

			- update (bool, default=True): Update records that already exist.

			- insert (bool, default=True): Insert new records.

			- conflict_on (str or list, default='Id'): Specify unique column(s) constrain
				to separate between update and insert.

				** These are case sensitive and have to match the Salesforce fields.

				** Numeric types are not supported for this argument.

			- return_response (bool, default=False): Include in response_payload
				the raw response from the Salesforce Api.

			- verbose (bool, default=False): Print query or queries.

		Returns:
			- response_payload (dict)

		'''
		dataframe = dataframe.copy()

		if isinstance(conflict_on, str) and conflict_on.lower() == 'id':

			# Id cannot be supplied for creating records.

			data = self.map_types(
				df=dataframe,
				tablename=tablename,
				return_as_dict=True
			)

			update_result = self.update(
				tablename=tablename,
				record_list=data,
				return_result=return_response
			)

			return update_result

		# Query logic for multi-value conlict_on
		conflict_flag = True if isinstance(conflict_on, list) else False

		if conflict_flag:

			# Check if any of the conflict_on columns are null.
			if dataframe[conflict_on].isnull().values.any():
				raise ValueError(
					'Conflict_on columns cannot be null.'
				)

			for col in conflict_on:
				dataframe[col] = dataframe[col].str.replace(
					"\\", "", regex=True).str.replace("'", "\\'")

			conflict_on_list = dataframe[conflict_on].to_dict(orient='records')

			conflict_on_list_fmt = []

			for i in conflict_on_list:
				fmt_list = []

				for k, j in i.items():
				   fmt_list.append(f"{k} = '{j}' ") 

				fmt_list = "(" + "AND ".join(fmt_list) + ")"
				conflict_on_list_fmt.append(fmt_list)

			conflict_on_list = conflict_on_list_fmt

		else:
			# For simple conflict on.
			conflict_on_list = dataframe[conflict_on].str.replace(
				"\\", "", regex=True).str.replace("'", "\\'").tolist()

		chunks = type(self).chunker(
			conflict_on_list,
			type(self).MAX_QUERY_SIZE
		)

		all_results = []

		for chunk in chunks:

			if conflict_flag:
				conflict_on_list = ' OR '.join(chunk)

			else:
				conflict_on_list = ','.join([f"'{i}'" for i in chunk])

			query = f'''
				SELECT
					Id,
					{', '.join(conflict_on) if conflict_flag else conflict_on}
				FROM
					{tablename}
				WHERE
					isdeleted = FALSE
			'''

			if conflict_flag:
				query += f" AND ({conflict_on_list})"

			else:
				query += f" AND {conflict_on} IN ({conflict_on_list}) "

			
			if verbose:
				print(query)

			results = self.sf.query_all(
				query,
				include_deleted=True,
				timeout=None
			)['records']

			results = pd.DataFrame.from_dict(results)
			
			all_results.append(results)

		results = pd.concat(all_results, axis=0, ignore_index=True)

		columns = ["Id"] + conflict_on if conflict_flag else ["Id", conflict_on]

		if results.shape[0] > 0:
			results_mapping =  results[columns]

		else:
			results_mapping = pd.DataFrame(columns=columns)

		dataframe = pd.merge(
			dataframe,
			results_mapping,
			on=conflict_on,
			how='left',
			suffixes=('', '_y')
		)

		existing_df = dataframe[
			~dataframe['Id'].isnull()
		]

		print('Existing records: ', existing_df.shape[0])

		# Settings objects to update and insert
		bulk_handler = getattr(self.sf, "bulk")
		bulk_object = getattr(bulk_handler,  tablename)

		response_payload = {}

		new_df = dataframe[
			dataframe['Id'].isnull()
		].drop('Id', axis=1)

		print('New records: ', new_df.shape[0])

		# Existing records
		if update:
			existing_data = self.map_types(
				df=existing_df,
				tablename=tablename,
				return_as_dict=True
			)

			existing_results = bulk_object.update(
				data=existing_data,
				batch_size=batch_size
			)

			conflict_on_list = conflict_on if conflict_flag else [conflict_on]

			for i, j in zip(existing_data, existing_results):

				for c in conflict_on_list:
					j[c] = i[c]

			response_payload['update'] = {'success': 0, 'failure': 0}

			for i in existing_results:
				if i['success'] == True:
					response_payload['update']['success'] += 1

				elif i['success'] == False:
					response_payload['update']['failure'] += 1

			if return_response:
				response_payload['update']['result'] = existing_results

		# New records
		if insert:
			new_data = self.map_types(
				df=new_df,
				tablename=tablename,
				return_as_dict=True
			)
			
			new_results = bulk_object.insert(
				data=new_data,
				batch_size=batch_size
			)

			conflict_on_list = conflict_on if conflict_flag else [conflict_on]

			for i, j in zip(new_data, new_results):
				for c in conflict_on_list:
					j[c] = i[c]

			response_payload['insert'] = {'success': 0, 'failure': 0}

			for i in new_results:
				if i['success'] == True:
					response_payload['insert']['success'] += 1

				elif i['success'] == False:
					response_payload['insert']['failure'] += 1

			if return_response:
				response_payload['insert']['result'] = new_results

		return response_payload

	def upsert_df(
		self,
		tablename,
		dataframe: pd.DataFrame,
		batch_size: int=10_000,
		update: bool=True,
		insert: bool=True,
		conflict_on: str or List[str]='Id',
		return_response: bool=False,
		overwrite: bool=False,
		overwrite_columns: str or List[str]=None,
		verbose: bool=False,
		update_diff_on: List[str]=None,
	):
		'''
		Args:
			- tablename (str): SObject in Salesforce

			- dataframe (pd.DataFrame): Data dataframe.

			- batch_size (int, default=10_000): Salesforce default.

			- update (bool, default=True): Update records that already exist.

			- insert (bool, default=True): Insert new records.

			- conflict_on (str or list, default='Id'): Specify unique column(s) constrain
				to separate between update and insert.

				** These are case sensitive and have to match the Salesforce fields.

			- return_response (bool, default=False): Include in response_payload
				the raw response from the Salesforce Api.

			- overwrite (bool, default=False): Overwrite existing records.
				If False, only values will be updated where there is currently
				no value.

			- overwrite_columns (str or list, default=None): Columns to overwrite,
				if overwrite is True. If None, all columns will be overwritten
				where a value exists.

			- verbose (bool, default=False): Compatibility with older version.
				No use.

			- update_diff_on (list, default=None): Update only where these
				columns are different from current and new.

		Returns:
			- response_payload (dict)

		'''
		### PART 1: Separate new from old records
		# Create copy of dataframe to avoid modifying original
		dataframe = dataframe.copy()

		# Settings objects to update and insert
		bulk_handler = getattr(self.sf, "bulk")
		bulk_object = getattr(bulk_handler,  tablename)

		# making conflict_on a list, not mandatory but useful of iteration
		if type(conflict_on) is str:
			conflict_on = [conflict_on]

		# Making overwrite_columns a list
		if type(overwrite_columns) is str:
			overwrite_columns = [overwrite_columns]

		# Using mapping to get SF Columns and proper types
		dataframe = self.map_types(
			df=dataframe,
			tablename=tablename,
			check_column_casing=True,
			return_as_dict=False
		)

		# Droping duplicates
		dataframe.drop_duplicates(subset=conflict_on, inplace=True)

		# Workaround regardless of whether id is in dataframe or not
		columns = list(set(dataframe.columns.tolist() + ['Id']))

		# Getting current data
		sf_df = self.query_parallel(
			tablename,
			columns=columns,
			limit=None,
			df=True,
			date_window=None,
			date_window_variable='LastModifiedDate',
			verbose=False,
			include_deleted=False,
			n_chunks=5
		)

		# Middle step to merge on conflict_on and separate new to existing.
		sf_df.columns = [
			f"{c}__current" if c not in conflict_on else c for c in sf_df.columns
		]

		# Middle step to strip conlict_on columns for both dataframes
		for col in conflict_on:

			dataframe[col] = dataframe[col].apply(
				lambda x: x.strip() if isinstance(x, str) else x
			)

			sf_df[col] = sf_df[col].apply(
				lambda x: x.strip() if isinstance(x, str) else x
			)

		merged_df = pd.merge(
			sf_df,
			dataframe,
			on=conflict_on,
			how='right',
			# suffixes=('', ''),
			indicator=True
		)

		# New df
		new_df = merged_df[merged_df['_merge'] == 'right_only']
		new_df = new_df[dataframe.columns.tolist()]
		print(f'New records: {new_df.shape[0]}')

		# Existing df
		existing_df = merged_df[merged_df['_merge'] == 'both']
		existing_df.drop('_merge', axis=1, inplace=True)
		print(f'Existing records: {existing_df.shape[0]}')

		# Only updating where there is a difference on update_diff_on
		if update_diff_on is not None:
			selected_indexes = []
			
			for c in update_diff_on:
				subselection = existing_df.loc[
					(existing_df[c].astype(str) != existing_df[f"{c}__current"].astype(str))
					& (existing_df[c].notnull())
					& (existing_df[c] != '')
				, :].index.tolist()

				selected_indexes.extend(subselection)

			existing_df = existing_df.loc[
				existing_df.index.isin(selected_indexes)
			]

			print('Updatedable records: ', existing_df.shape[0])

		### PART 2: Strategy for updating value and overwrite
		comparison_columns = [
			c for c in columns if c not in set(conflict_on) | {'Id'}
		]

		for column in comparison_columns:

			if not overwrite:
				existing_df[f"{column}__current"].fillna(
					existing_df[column],
					inplace=True
				)

			elif overwrite and overwrite_columns is None:
				existing_df[column].fillna(
					existing_df[f"{column}__current"],
					inplace=True
				)

			else:
				if column in overwrite_columns:
					existing_df[column].fillna(
						existing_df[f"{column}__current"],
						inplace=True
					)
					existing_df[f"{column}__current"] = existing_df[column]

				else:
					existing_df[f"{column}__current"].fillna(
						existing_df[column],
						inplace=True
					)

		if overwrite and overwrite_columns is None:
			existing_df = existing_df[columns]

		else:
			existing_df = existing_df[ 
				[c for c in existing_df.columns if c.endswith('__current')]
				+
				conflict_on
			]
			existing_df.columns = [
				c.replace('__current', '') for c in existing_df.columns
			]

		### PART 3: Update and insert records
		response_payload = {}

		if insert and not new_df.empty:

			new_data = self.map_types(
				df=new_df,
				tablename=tablename,
				return_as_dict=True,
				check_column_casing=True
			)

			new_results = bulk_object.insert(
				data=new_data,
				batch_size=batch_size
			)

			for i, j in zip(new_data, new_results):
				for c in conflict_on:
					if c.lower() != 'id':
						j[c] = i[c]

			response_payload['insert'] = {'success': 0, 'failure': 0}

			for i in new_results:
				if i['success'] == True:
					response_payload['insert']['success'] += 1

				elif i['success'] == False:
					response_payload['insert']['failure'] += 1

			if return_response:
				response_payload['insert']['result'] = new_results

		if update and not existing_df.empty:

			existing_data = self.map_types(
				df=existing_df,
				tablename=tablename,
				return_as_dict=True,
				check_column_casing=True
			)

			existing_results = bulk_object.update(
				data=existing_data,
				batch_size=batch_size
			)

			for i, j in zip(existing_data, existing_results):

				for c in conflict_on:
					if c.lower() != 'id':
						j[c] = i[c]

			response_payload['update'] = {'success': 0, 'failure': 0}

			for i in existing_results:
				if i['success'] == True:
					response_payload['update']['success'] += 1

				elif i['success'] == False:
					response_payload['update']['failure'] += 1

			if return_response:
				response_payload['update']['result'] = existing_results

		return response_payload

	# def insert_df(
	# 	self,
	# 	tablename: str,
	# 	dataframe: pd.DataFrame,
	# 	batch_size: int=10_000,
	# 	update=True,
	# 	insert=True,
	# 	conflict_on: str or List[str] ='Name',
	# 	return_response=False,
	# ):
	# 	'''
	# 	Args:
	# 		- tablename (str): SObject in Salesforce
	#
	# 		- dataframe (pd.DataFrame): Data dataframe.
	#
	# 		- batch_size (int, default=10_000): Salesforce default.
	#
	# 		- update (bool, default=True): Update records that already exist.
	#
	# 		- insert (bool, default=True): Insert new records.
	#
	# 		- conflict_on (str or list, default='Name'): Specify unique column(s) constrain
	# 			to separate between update and insert.
	#
	# 			** These are case sensitive and have to match the Salesforce fields.
	#
	# 			** Numeric types are not supported for this argument.
	#
	# 		- return_response (bool, default=False): Include in response_payload
	# 			the raw response from the Salesforce Api.
	#
	# 	Returns:
	# 		- response_payload (dict)
	#
	# 	'''
	# 	if not self.check_table_exists(tablename):
	# 		return None
	#
	# 	dataframe = dataframe.copy()
	#
	# 	conflict_flag = True if isinstance(conflict_on, list) else False
	#
	# 	# STEP 1: Separate new from existing according to "conflict_on"
	#
	# 	# Query logic for multi-value conlict_on
	# 	if conflict_flag:
	#
	# 		for col in conflict_on:
	# 			dataframe[col] = dataframe[col].str.replace("'", "\\'")
	#
	# 		conflict_on_list = dataframe[conflict_on].to_dict(orient='records')
	#
	# 		conflict_on_list_fmt = []
	#
	# 		for i in conflict_on_list:
	# 			fmt_list = []
	#
	# 			for k, j in i.items():
	# 			   fmt_list.append(f"{k} = '{j}' ") 
	#
	# 			fmt_list = "(" + "AND ".join(fmt_list) + ")"
	# 			conflict_on_list_fmt.append(fmt_list)
	#
	# 		conflict_on_list = conflict_on_list_fmt
	#
	# 	else:
	# 		# For simple conflict on.
	# 		conflict_on_list = dataframe[conflict_on].str.replace("'", "\\'").tolist()
	#
	# 	# Query as a maximum for "WHERE IN"
	# 	def chunker(seq, size):
	# 		return (seq[pos:pos + size] for pos in range(0, len(seq), size))
	#
	# 	chunks = chunker(
	# 		conflict_on_list,
	# 		type(self).MAX_QUERY_SIZE
	# 	)
	#
	# 	all_results = []
	#
	# 	for chunk in chunks:
	#
	# 		if conflict_flag:
	# 			conflict_on_list = ' OR '.join(chunk)
	#
	# 		else:
	# 			conflict_on_list = ','.join([f"'{i}'" for i in chunk])
	#
	# 		query = f'''
	# 			SELECT
	# 				Id,
	# 				{', '.join(conflict_on) if conflict_flag else conflict_on}
	# 			FROM
	# 				{tablename}
	# 			WHERE
	# 				isdeleted = FALSE
	# 		'''
	#
	# 		if conflict_flag:
	# 			query += f" AND ({conflict_on_list})"
	#
	# 		else:
	# 			query += f" AND {conflict_on} IN ({conflict_on_list}) "
	#
	# 		results = self.sf.query_all(
	# 			query,
	# 			include_deleted=True,
	# 			timeout=None
	# 		)['records']
	#
	# 		results = pd.DataFrame.from_dict(results)
	# 		
	# 		all_results.append(results)
	#
	# 	results = pd.concat(all_results, axis=0, ignore_index=True)
	#
	# 	columns = ["Id"] + conflict_on if conflict_flag else ["Id", conflict_on]
	#
	# 	if results.shape[0] > 0:
	# 		results_mapping =  results[columns]
	#
	# 	else:
	# 		results_mapping = pd.DataFrame(columns=columns)
	#
	# 	dataframe = pd.merge(
	# 		dataframe,
	# 		results_mapping,
	# 		on=conflict_on,
	# 		how='left',
	# 		suffixes=('', '_y')
	# 	)
	#
	# 	existing_df = dataframe[
	# 		~dataframe['Id'].isnull()
	# 	]
	#
	# 	print('Existing records: ', existing_df.shape[0])
	#
	# 	new_df = dataframe[
	# 		dataframe['Id'].isnull()
	# 	].drop('Id', axis=1)
	#
	# 	print('New records: ', new_df.shape[0])
	#
	# 	# Settings objects to update and insert
	# 	bulk_handler = getattr(self.sf, "bulk")
	# 	bulk_object = getattr(bulk_handler,  tablename)
	#
	# 	response_payload = {}
	#
	# 	# Existing records
	# 	if update:
	# 		existing_data = self.map_types(
	# 			df=existing_df,
	# 			tablename=tablename,
	# 			return_as_dict=True
	# 		)
	#
	# 		existing_results = bulk_object.update(
	# 			data=existing_data,
	# 			batch_size=batch_size
	# 		)
	#
	# 		conflict_on_list = conflict_on if conflict_flag else [conflict_on]
	#
	# 		for i, j in zip(existing_data, existing_results):
	#
	# 			for c in conflict_on_list:
	# 				j[c] = i[c]
	#
	# 		response_payload['update'] = {'success': 0, 'failure': 0}
	#
	# 		for i in existing_results:
	# 			if i['success'] == True:
	# 				response_payload['update']['success'] += 1
	#
	# 			elif i['success'] == False:
	# 				response_payload['update']['failure'] += 1
	#
	# 		if return_response:
	# 			response_payload['update']['results'] = existing_results
	#
	# 	# New records
	# 	if insert:
	# 		new_data = self.map_types(
	# 			df=new_df,
	# 			tablename=tablename,
	# 			return_as_dict=True
	# 		)
	# 		
	# 		new_results = bulk_object.insert(
	# 			data=new_data,
	# 			batch_size=batch_size
	# 		)
	#
	# 		conflict_on_list = conflict_on if conflict_flag else [conflict_on]
	#
	# 		for i, j in zip(new_data, new_results):
	# 			for c in conflict_on_list:
	# 				j[c] = i[c]
	#
	# 		response_payload['insert'] = {'success': 0, 'failure': 0}
	#
	# 		for i in new_results:
	# 			if i['success'] == True:
	# 				response_payload['insert']['success'] += 1
	#
	# 			elif i['success'] == False:
	# 				response_payload['insert']['failure'] += 1
	#
	# 		if return_response:
	# 			response_payload['insert']['results'] = new_results
	#
	# 	return response_payload









