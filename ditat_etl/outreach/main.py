import time

from datetime import datetime, timedelta
import requests
import pandas as pd
from bs4 import BeautifulSoup

from .scopes import scopes
from ..utils.functions import sanitize_join_values


class Outreach:

	OUTREACH_TO_PYTHON = {
		'string': str,
		'integer': int,
		'float': float,
		'boolean': bool,
		'date-time': 'datetime64',
		'date': 'datetime64',
		'object': dict,
		'email': str,

	}

	BASE_URL = 'https://api.outreach.io'

	OAUTH_URL = f'{BASE_URL}/oauth'

	API_URL = f'{BASE_URL}/api/v2'

	SCOPES = scopes

	RESOURCES = {
		'calls': {
			'singular': 'call'
		},
		'mailings': {
			'singular': 'mailing'
		},
		'users': {
			'singular': 'user'
		},
		'mailboxes': {
			'singular': 'mailbox'
		},
		'prospects': {
			'singular': 'prospect'
		},
		'accounts': {
			'singular': 'account'
		},
		'opportunities': {
			'singular': 'opportunity'
		},
		'tasks': {
			'singular': 'task'
		},
		'callDispositions': {
			'singular': 'callDisposition'
		},
		'callPurposes': {
			'singular': 'callPurpose'
		},
	}

	def __init__(
		self,
		client_id: str,
		client_secret: str,
		refresh_token: str=None,
		redirect_uri: str=None
		):
		self.client_id = client_id

		self.client_secret = client_secret

		self.refresh_token = refresh_token

		self.access_token = None

		self.redirect_uri = redirect_uri

	def authorize(self, scopes: list=None):
		"""
		Authorize the application to access the Outreach API.

		This is not really an endpoint, but rather and endpoint that needs
		to be called in the browser to get an authorization code.

		"""

		url = f'{self.OAUTH_URL}/authorize'

		params = {
			'client_id': self.client_id,
			'response_type': 'code',
			'redirect_uri': self.redirect_uri,
			'scope': ' '.join(scopes) if scopes else ' '.join(self.SCOPES),
		}

		fmt_url = requests.Request('GET', url, params=params).prepare().url

		print(fmt_url)

		return fmt_url

	@classmethod
	def get_resource_info(
		cls,
		resource: str,
		return_as_df: bool=True,
		):
		"""
		Get the information about a resource.

		Args:

			- resource (str): The name of the resource.

		"""
		print(f"Getting data types about the {resource} resource.")

		url = f'{cls.API_URL}/docs'

		response = requests.get(url)

		soup = BeautifulSoup(response.text, 'lxml')

		h3 = soup.find('h3', id=resource)

		attributes = h3.find_next('table')
		relationships = attributes.find_next('table')
		
		attributes = pd.read_html(str(attributes))[0]
		relationships = pd.read_html(str(relationships))[0]

		attributes[['name', 'type']] = attributes['Attribute Name'].str.split(' ', 1, expand=True)
		attributes = attributes[['name', 'type']]

		relationships['name'] = relationships['Relationship Name']
		relationships['type'] = 'string'
		relationships = relationships[['name', 'type']]

		if return_as_df:

			attributes['name'] = 'attributes_' + attributes['name']

			relationships['name'] = 'relationships_' + relationships['name']

			df = pd.concat([attributes, relationships], axis=0)

			df['python_type'] = df['type'].map(cls.OUTREACH_TO_PYTHON)

			return df

		else:
			result = {
				'attributes': attributes.to_dict('records'),
				'relationships': relationships.to_dict('records'),
				'resource': resource,
		}

		return result

	def token(self, value: str, grant_type: str='refresh_token'):
		"""
		Get an access token.

		Args:
			
			- value (str): The value of the grant type.

			- grant_type (str, default='refresh_token'): The type of grant.
				Possible values are 'refresh_token' and 'authorization_code'.

		"""

		url = f'{self.OAUTH_URL}/token'

		params = {
			'client_id': self.client_id,
			'client_secret': self.client_secret,
			'redirect_uri': self.redirect_uri,
			'grant_type': grant_type,
		}

		if grant_type == 'refresh_token':
			params['refresh_token'] = value

		elif grant_type == 'authorization_code':
			params['code'] = value

		print(f"Getting an access token with grant type {grant_type}.")

		response = requests.post(url, data=params)

		if response.status_code != 200:
			print(response.text)
			return

		result = response.json()

		self.access_token = result['access_token']

		return result

	@staticmethod
	def date_range(start, end, intv, fmt='%Y-%m-%d'):
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

			yield (start + diff * i).strftime('%Y-%m-%dT%H:%M:%SZ')

		yield end.strftime('%Y-%m-%dT%H:%M:%SZ')

	def get_resource(
		self, 
		resource: str,
		return_as_dataframe: bool=True,
		date_window: int=7,
		date_from: str=None,
		date_to: str=None,
		date_variable: str='updatedAt',
		chunk_size: int=50,
		date_fmt: str='%Y-%m-%d',
		**kwargs
		):
		"""
		Get the resource.

		Args:
			
			- resource (str): The resource to get.

			- return_as_dataframe (bool, default=True): Whether to return
				the result as a pandas DataFrame.

			- date_window (int, default=7): The number of days to get the
				resource for.

			- date_from (str, default=None): The date from which to get
				the resource. Format is YYYY-MM-DD.

			- date_to (str, default=None): The date to which to get the
				resource. Format is YYYY-MM-DD.

			- date_variable (str, default='updatedAt'): The variable to
				use for the date range. Other possible values are
				'createdAt' and 'deletedAt'.

			- chunk_size (int, default=50): The number of records to get
				at a time.

			- date_fmt (str, default='%Y-%m-%d'): The date format.

		"""

		if resource not in self.RESOURCES:
			print(f'Invalid resource: {resource}')
			return

		if date_from or date_to:

			if not date_from:
				date_from = '2000-01-01T00:00:00Z'

			else:
				date_from = datetime.strptime(date_from, date_fmt).strftime('%Y-%m-%dT%H:%M:%SZ')

			if not date_to:
				date_to = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

			else:
				date_to = datetime.strptime(date_to, date_fmt).strftime('%Y-%m-%dT%H:%M:%SZ')

		else:

			date_from = (datetime.now() - timedelta(days=date_window)).strftime('%Y-%m-%dT%H:%M:%SZ')

			date_to = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%dT%H:%M:%SZ')

		params = {
			f"filter[{date_variable}]": f"{date_from}..{date_to}",
			"page[limit]": chunk_size,
		}

		params.update(kwargs)

		# Getting an access token if we don't have one.
		for _ in range(10):

			if not self.access_token:
				self.token(self.refresh_token)
				time.sleep(1)

			else:
				break

		print(f"Getting {resource} using {date_variable} from [{date_from}] to [{date_to}].")

		url = f'{self.API_URL}/{resource}'

		headers = {
			'Authorization': f'Bearer {self.access_token}',
		}

		response = requests.get(url, headers=headers, params=params)

		if response.status_code != 200:
			print(response.text)
			return

		result = response.json()

		total = result['meta']['count']
		
		print(f"Got {total} {resource}.")

		if total > 10_000:
			print('WARNING: The total number of records is greater than 10,000.')

			date_range = self.date_range(
				date_from,
				date_to,
				int(total / 1000),
				fmt='%Y-%m-%dT%H:%M:%SZ',
			)
			date_range = list(date_range)

			print(f"Splitting date range in {int(total / 1000)} batches.")

			df_list = []

			for i, v in enumerate(date_range[:-1]):

				df = self.get_resource(
					resource=resource,
					date_window=None,
					date_from=v,
					date_to=date_range[i + 1],
					date_fmt='%Y-%m-%dT%H:%M:%SZ',
					date_variable=date_variable,
				)

				df_list.append(df)

			df = pd.concat(df_list)

			return df

		for i in range(1, total // 50 + 1):

			print(f"Getting page {i + 1} of {total // 50 + 1}.", end='\r')

			params['page[offset]'] = i * 50

			response = requests.get(url, headers=headers, params=params)

			if response.status_code != 200:
				print(response.text)
				return

			result['data'] += response.json()['data']

		if return_as_dataframe:
			result = pd.json_normalize(result['data'])

			# replace periods
			result.columns = [c.replace('.', '_') for c in result.columns]

			# Datetimes
			datetime_columns = [c for c in result.columns if c.endswith('At')]

			for c in datetime_columns:
				result[c] = pd.to_datetime(result[c], format='%Y-%m-%dT%H:%M:%S.000Z')

			# Ids
			id_columns = [
				c for c in result.columns if c.endswith('Id') or c.lower() == 'id'
			]

			for c in id_columns:
				result[c] = sanitize_join_values(result[c])

		return result
			

			


