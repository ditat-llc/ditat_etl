from datetime import datetime, timedelta
import requests
import pandas as pd

from .scopes import scopes


class Outreach:

	BASE_URL = 'https://api.outreach.io'

	OAUTH_URL = f'{BASE_URL}/oauth'

	API_URL = f'{BASE_URL}/api/v2'

	SCOPES = scopes

	RESOURCES = [
		'calls',
		'mailings',
		'users',
		'mailboxes',
		'prospects',
	]

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

	def get_resource(
		self, 
		resource: str,
		return_as_dataframe: bool=True,
		date_window: int=7,
		date_from: str=None,
		date_to: str=None,
		date_variable: str='updatedAt',
		chunk_size: int=50,
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
		"""

		if resource not in self.RESOURCES:
			print(f'Invalid resource: {resource}')
			return

		if date_from or date_to:

			if not date_from:
				date_from = 'neginf'

			if not date_to:
				date_to = 'inf'

		else:

			date_from = (datetime.now() - timedelta(days=date_window)).strftime('%Y-%m-%dT%H:%M:%SZ')

			date_to = 'inf'

		params = {
			f"filter[{date_variable}]": f"{date_from}..{date_to}",
			"page[limit]": chunk_size,
		}

		params.update(kwargs)

		# Getting an access token if we don't have one.
		if not self.access_token:
			self.token(self.refresh_token)

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

		return result
			

			


