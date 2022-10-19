from datetime import datetime, timedelta
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
	}

	VERSION = 'v3'

	BASE_URL = 'https://api.hubapi.com'

	OBJECTS = {
		'companies': {
			"date_column": "hs_lastmodifieddate",
		},
		'contacts': {
			"date_column": "lastmodifieddate",
		},
		'deals': {
			"date_column": "hs_lastmodifieddate",
		},
	}

	def __init__(self, api_key):
		self.api_key = api_key

	def get_object_info(self, object_type):
		url = f"{self.BASE_URL}/crm/{self.VERSION}/properties/{object_type}"

		headers = {
			'Authorization': f'Bearer {self.api_key}',
			'Content-Type': 'application/json',
		}

		response = requests.get(url, headers=headers)

		if response.status_code != 200:
			return None

		resp = response.json()

		df = pd.DataFrame(resp['results'])

		return df

	def get_object_types(self, object_type):
		df = self.get_object_info(object_type)

		if df is None:
			return None

		df = df[['name', 'type']]
		df['original_type'] = df['type']

		df['type'] = df['type'].apply(lambda x: self.TYPES_MAPPING[x])

		df = df.set_index('name')

		return df

	def get_object_columns(self, object_type):
		df = self.get_object_info(object_type)

		if df is None:
			return None

		return df['name'].tolist()

	def map_types(self, object_type, df):
		df = df.copy()

		object_type_df = self.get_object_types(object_type)

		df = df.astype(object_type_df.to_dict()['type'], errors='ignore')

		df.replace(to_replace=['None'], value=np.nan, inplace=True)

		return df

	def query(
		self,
		object_type: str,
		date_window: float=1,
		date_column: str=None,
		limit: int=100,
		start_date: str=None,
		end_date: str=None
	):
		'''
		Args:

			- object_type (str): The object type to query.

			- date_window (float, default=1): The date window to query.

			- date_column (str, default=None): The date column to query.

			- limit (int, default=100): The number or records per batch.

			- start_date (str, default=None): Format: YYYY/MM/DD.

			- end_date (str, default=None): Format: YYYY/MM/DD.

		Returns:
			
			- df (pd.DataFrame): The query results.

		Notes:
			
			- If start_date and end_date are not provided, the function will
			query the last date_window days.
		
		'''

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

			end_date = datetime.strptime(end_date, '%Y/%m/%d')

		end_date = end_date.timestamp() * 1000

		if start_date is None:

			start_date = (datetime.now().timestamp() - date_window * 24 * 60 * 60) * 1000

		else:
			
			start_date = datetime.strptime(start_date, '%Y/%m/%d').timestamp() * 1000

		start_date = int(start_date)
		end_date = int(end_date)

		data = {
			'filters': [
				{
					'propertyName': date_column,
					# 'operator': 'GTE',
					'operator': 'BETWEEN',
					'value': start_date,
					'highValue': end_date,

				}
			],
			"sorts": [
				{
					"propertyName": date_column,
					"direction": "DESCENDING"
				}
			],
			"properties": self.get_object_columns(object_type),
		}

		data['limit'] = limit
		data['archived'] = 'false'

		response = requests.post(url, headers=headers, json=data)

		if response.status_code != 200:
			return None

		result = response.json()

		total = result['total']
		results = result['results']

		start_date_fmt = datetime.fromtimestamp(start_date / 1000).strftime('%Y/%m/%dT%H:%M:%S')

		end_date_fmt = datetime.fromtimestamp(end_date / 1000).strftime('%Y/%m/%dT%H:%M:%S')

		print(f"Total {object_type} from [{start_date_fmt}] to [{end_date_fmt}]: {total}")

		df_list = []

		df_list.extend(results)

		for i in range(len(results), total + 1, len(results)):
			data['after'] = i

			response = requests.post(
				url,
				headers=headers,
				json=data,
			)

			df_list.extend(response.json()['results'])
			
		df = pd.json_normalize(df_list)

		df.drop_duplicates(subset=['id'], inplace=True)

		df = df[['id'] + [c for c in df.columns if c.startswith('properties.')]]
		df.columns = [c.replace('properties.', '') for c in df.columns]

		df = self.map_types(object_type=object_type, df=df)

		return df

