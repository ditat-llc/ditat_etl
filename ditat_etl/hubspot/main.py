import json
from datetime import datetime, timedelta
from typing import Union

import requests
import pandas as pd
import numpy as np


# https://developers.hubspot.com/docs/api/crm/search

class Hubspot:
	TYPES_MAPPING = {
		'string': str,
		'number': float,
		'enumeration': str,
		'datetime': 'datetime64',
		'date': 'datetime64',
		'bool': bool,
		'phone_number': str,
		'json': dict,
	}

	VERSION = 'v3'

	BASE_URL = 'https://api.hubapi.com'

	OBJECTS = {
		'companies': {
			"date_column": "hs_lastmodifieddate",
			"singular": "company",
		},
		'contacts': {
			"date_column": "lastmodifieddate",
			"singular": "contact",
		},
		'deals': {
			"date_column": "hs_lastmodifieddate",
			"singular": "deal",
		},
		'tickets': {
			"date_column": "hs_lastmodifieddate",
			"singular": "ticket",
		},
		'tasks': {
			"date_column": "hs_lastmodifieddate",
			"singular": "task",
		},
		'calls': {
			"date_column": "hs_lastmodifieddate",
			"singular": "call",
		},
		'emails': {
			"date_column": "hs_lastmodifieddate",
			"singular": "email",
		},
		'meetings': {
			"date_column": "hs_lastmodifieddate",
			"singular": "meeting",
		},
		'notes': {
			"date_column": "hs_lastmodifieddate",
			"singular": "note",
		},
		'postal_mail': {
			"date_column": "hs_lastmodifieddate",
			"singular": "postal_mail",
		},
		'owners': {},
	}

	OWNER_TYPES = {
		"id": int,
		"email": str,
		"firstName": str,
		"lastName": str,
		"userId": int,
		"createdAt": 'datetime64',
		"updatedAt": 'datetime64',
		"archived": bool,
		"teams": list,
	}

	DEFAULT_DATE_WINDOW = 0.1

	def __init__(self, api_key):
		'''
		Args:

			- api_key (str): Hubspot API key
		'''
		self.api_key = api_key

	def get_object_info(self, object_type, return_as_df=True):
		'''
		Args:

			- object_type (str): Hubspot object type

			- return_as_df (bool): Return as pandas DataFrame

		Returns:

			- df (pd.DataFrame) or dict: Hubspot object info	
		'''
		url = f"{self.BASE_URL}/crm/{self.VERSION}/properties/{object_type}"

		headers = {
			'Authorization': f'Bearer {self.api_key}',
			'Content-Type': 'application/json',
		}

		response = requests.get(url, headers=headers)

		if response.status_code != 200:
			print(response.text)
			return None

		resp = response.json()

		df = pd.DataFrame(resp['results'])

		if return_as_df is False:
			return df.to_dict(orient='records')

		return df

	def get_object_types(self, object_type, return_as_df=True):
		'''
		Based on self.get_object_info

		Args:

			- object_type (str): Hubspot object type

			- return_as_df (bool): Return as pandas DataFrame

		Returns:

			- df (pd.DataFrame) or dict: Hubspot object types
		'''
		df = self.get_object_info(object_type)

		if df is None:
			return None

		df = df[['name', 'type']]

		df['original_type'] = df['type']

		df['type'] = df['type'].apply(lambda x: self.TYPES_MAPPING[x])

		df = df.set_index('name')

		if return_as_df is False:
			return df.to_dict(orient='records')

		return df

	def get_associations(self, from_object, to_object, ids, return_as_df=True):
		url = f"{self.BASE_URL}/crm/{self.VERSION}/associations/{from_object}/{to_object}/batch/read"

		headers = {
			'Authorization': f'Bearer {self.api_key}',
			'Content-Type': 'application/json',
		}

		data = {
			"inputs": ids
		}

		response = requests.post(url, headers=headers, data=json.dumps(data))

		if response.status_code not in [200, 207]:
			print(response.text)
			return None

		resp = response.json()['results']

		resp = [{
			"from": x['from']['id'], "to": x['to'][0]['id'], 'type': x['to'][0]['type']
		} for x in resp]

		if return_as_df is False:
			return resp

		return pd.DataFrame(resp)

	def get_object_columns(self, object_type):
		'''
	    Based on self.get_object_info

		Args:

			- object_type (str): Hubspot object type

		Returns:

			- columns (list): Hubspot object columns
		'''

		df = self.get_object_info(object_type)

		if df is None:
			return None

		columns =  df['name'].tolist()

		return columns

	def map_types(self, object_type, df):
		'''
		Args:

			- object_type (str): Hubspot object type

			- df (pd.DataFrame): DataFrame to map types

		Returns:

			- df (pd.DataFrame): DataFrame with mapped types
		'''
		df = df.copy()

		mappings = self.get_object_types(object_type).to_dict()['type']

		for col, dtype in mappings.items():

			if col in df.columns:

				if dtype in [dict, list]:
					df[col] = df[col].apply(lambda x: json.dumps(x))

				else:
				
					df[col] = df[col].astype(dtype, errors='ignore')

		df.replace(to_replace=['None', ''], value=np.nan, inplace=True)

		return df

	@staticmethod
	def date_range(start, end, intv, fmt='%Y/%m/%d'):
		'''
		Function to split date range into intervals.
		This is used to split the start and end date when the total number
		of records is greater than 10,000.

		Args:

			- start (str): Start date

			- end (str): End date

			- intv (int): Interval

			- fmt (str): Date format

		Returns:

			- date_range (iter): Date range iterator
		'''
		start = datetime.strptime(start, fmt)

		end = datetime.strptime(end, fmt)

		diff = (end  - start ) / intv

		for i in range(intv):

			yield (start + diff * i).strftime('%Y/%m/%dT%H:%M:%S')

		yield end.strftime('%Y/%m/%dT%H:%M:%S')

	def get_owners(self):
		'''
		Returns:

			- df (pd.DataFrame): Hubspot owners
		'''
		url = f"{self.BASE_URL}/crm/{self.VERSION}/owners"

		headers = {
			'Authorization': f'Bearer {self.api_key}',
			'Content-Type': 'application/json',
		}

		params = {'limit': 100}

		result_list = []

		response = requests.get(url, headers=headers, params=params)

		if response.status_code != 200:
			print(response.text)
			return

		results = response.json()['results']

		result_list.extend(results)

		next_page = response.json().get('paging', {}).get('next', {}).get('link')

		while next_page is not None:

			response = requests.get(next_page, headers=headers)

			if response.status_code != 200:
				print(response.text)
				return

			results = response.json()['results']

			result_list.extend(results)

			next_page = response.json().get('paging', {}).get(
				'next', {}).get('link')

		df = pd.DataFrame(result_list)

		return df

	def query(
		self,
		object_type: str,
		date_window: float=None,
		date_column: str=None,
		limit: int=100,
		start_date: str=None,
		end_date: str=None,
		date_fmt: str='%Y/%m/%d',
		columns: list=None,
		hs_object_id: Union[str, list]=None,
		**kwargs
		):
		'''
		Args:

			- object_type (str): The object type to query.

			- date_window (float, default=1): The date window to query.

			- date_column (str, default=None): The date column to query.

			- limit (int, default=100): The number or records per batch.

			- start_date (str, default=None): Format: YYYY/MM/DD.

			- end_date (str, default=None): Format: YYYY/MM/DD.

			- date_fmt (str, default='%Y/%m/%d'): Date format.

			- columns (list, default=None): List of columns to query. If not provided,
				all columns will be queried.

			- hs_object_id (str | list, default=None): Hubspot object id(s).
				** All date filtering will be ignored if this is provided.

			- kwargs (dict): Additional query parameters for filtering

		Returns:
			
			- df (pd.DataFrame): The query results.

		Notes:
			
			- If start_date and end_date are not provided, the function will
			query the last date_window days.

			- When the total number of records is greater than 10,000,
			a new set is created of length N = total_records / 10,000.
			The function calls itself N times, allowing for recursion if necessary.

			- If object_type is 'owners', the function will return the owners.
		
		'''
		if object_type == 'owners':
			return self.get_owners()

		date_window = date_window or self.DEFAULT_DATE_WINDOW

		if object_type not in self.OBJECTS:
			raise ValueError(f'Object type must be one of {self.OBJECTS}')

		url = f"{self.BASE_URL}/crm/{self.VERSION}/objects/{object_type}/search"

		headers = {
			'Authorization': f'Bearer {self.api_key}',
			'Content-Type': 'application/json',
		}

		date_column = date_column or self.OBJECTS[object_type]['date_column']

		# Period logic
		if end_date is None:

			end_date = datetime.now() + timedelta(days=1)

		else:

			end_date = datetime.strptime(end_date, date_fmt)

		end_date = end_date.timestamp() * 1000

		if start_date is None:

			start_date = (datetime.now().timestamp() - date_window * 24 * 60 * 60) * 1000

		else:
			
			start_date = datetime.strptime(start_date, date_fmt).timestamp() * 1000

		start_date = int(start_date)

		end_date = int(end_date)

		start_date_fmt = datetime.fromtimestamp(start_date / 1000).strftime('%Y/%m/%dT%H:%M:%S')

		end_date_fmt = datetime.fromtimestamp(end_date / 1000).strftime('%Y/%m/%dT%H:%M:%S')

		# Payload
		data = {
			"filters": [],
			"properties": columns or self.get_object_columns(object_type),
			"sorts": [
				{
					"propertyName": date_column,
					"direction": "DESCENDING"
				}
			],
		}

		if hs_object_id is not None:
			
			if isinstance(hs_object_id, str):
				hs_object_id = [hs_object_id]

			data['filters'].append({
				'propertyName': 'hs_object_id',
				'operator': 'IN',
				'values': hs_object_id,
			})

		else:

			data['filters'].append({
					'propertyName': date_column,
					'operator': 'BETWEEN',
					'value': start_date,
					'highValue': end_date,
			})

		if kwargs:

			for key, value in kwargs.items():

				op_ = 'IN' if isinstance(value, list) else 'EQ'

				data['filters'].append(
					{
						'propertyName': key,
						'operator': op_,
						'values' if op_ == 'IN' else 'value': value,
					}
				)

		data['limit'] = limit
		data['archived'] = 'false'

		response = requests.post(url, headers=headers, json=data)

		if response.status_code != 200:
			print(response.text)
			return None

		result = response.json()

		total = result['total']
		results = result['results']

		if hs_object_id is not None:
			print(f"Querying {len(results)} records for {object_type} using hs_object_id.")

		else:
			print(f"Total {object_type} using {date_column} from [{start_date_fmt}] to [{end_date_fmt}]: {total}")

		if total == 0:
			return None

		if total > 10_000:
			# In this case we create more splits (n) and calls itself n times.

			print('WARNING: The total number of records is greater than 10,000.')

			date_range = self.date_range(
				start_date_fmt,
				end_date_fmt,
				int(total / 1000),
				fmt='%Y/%m/%dT%H:%M:%S',
			)
			date_range = list(date_range)

			print(f"Splitting date range in {int(total / 1000)} batches.")

			df_list = []

			for i, v in enumerate(date_range[:-1]):

				df = self.query(
					object_type=object_type,
					date_column=date_column,
					start_date=v,
					end_date=date_range[i + 1],
					date_fmt='%Y/%m/%dT%H:%M:%S',
					columns=columns,
				)

				df_list.append(df)

			df = pd.concat(df_list)

			return df

		df_list = []

		df_list.extend(results)

		for i in range(len(results), total + 1, len(results)):
			data['after'] = i

			response = requests.post(
				url,
				headers=headers,
				json=data,
			)

			if response.status_code != 200:
				print(response.text)
				continue

			df_list.extend(response.json()['results'])
			
		df = pd.json_normalize(df_list)

		df.drop_duplicates(subset=['id'], inplace=True)

		df = df[['id'] + [c for c in df.columns if c.startswith('properties.')]]
		df.columns = [c.replace('properties.', '') for c in df.columns]

		df = self.map_types(object_type=object_type, df=df)

		return df

	def upsert_record(
		self,
		object_type,
		method: str='create',
		association_object: str=None,
		association_name: str=None,
		association_id: str=None,
		**kwargs
		):
		'''
		Create a record in HubSpot.

		Args:

			- object_type (str)

			- method (str): The method to use to create the record. Can be 'create' or 'update'.

			- association_object (str): The object type to associate the record with.

			- association_name (str): The name of the association.

			- **kwargs: The record data.
		'''
		if method not in ['create', 'update']:
			raise ValueError(f"Invalid method: {method}. It must be 'create' or 'update'.")

		if object_type not in self.OBJECTS:
			raise ValueError(f'Object type must be one of {self.OBJECTS}')

		url = f"{self.BASE_URL}/crm/{self.VERSION}/objects/{object_type}"

		if method == 'update':
			id_ = kwargs.pop('id')

			print(f"Updating {object_type} with id {id_}")

			url += f"/{id_}"

			method = 'PATCH'

		else:
			method = 'POST'

			print(f"Creating {object_type}")

		payload = {'properties': kwargs}

		headers = {
			'Authorization': f'Bearer {self.api_key}',
			'Content-Type': 'application/json',
		}

		response = requests.request(method=method, url=url, headers=headers, data=json.dumps(payload))

		if str(response.status_code)[0] != '2':
			print(response.text)
			return None

		result = response.json()

		if association_object is None:
			return result

		id_ = result['id']

		association_object = self.OBJECTS[association_object]['singular']

		association_type = f"{self.OBJECTS[object_type]['singular']}_to_{association_object}"

		association_id = association_id or result['properties'].get(association_name, {})

		if not association_id:
			print(f"Could not find association id for {association_name}")
			return result

		association_url = f"{self.BASE_URL}/crm/{self.VERSION}/objects/{object_type}/{id_}/associations/{association_object}/{association_id}/{association_type}"

		association_response = requests.put(association_url, headers=headers)

		if association_response.status_code != 200:
			print(association_response.text)
			return None

		association_result = association_response.json()

		result['association'] = association_result

		return result





