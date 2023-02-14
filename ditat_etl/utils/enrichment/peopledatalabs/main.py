import os
from typing import Optional, List, Union, Dict
import time
from datetime import datetime
import json
from io import BytesIO, StringIO
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
import numpy as np
import boto3

from ....time import TimeIt
from ....url.functions import extract_domain


filedir = os.path.abspath(os.path.dirname(__file__))


class PeopleDataLabs:
	VERSION = 'v5'

	BASE_URL = f'https://api.peopledatalabs.com/{VERSION}'

	SANDOX_URL = f"https://sandbox.api.peopledatalabs.com/{VERSION}"

	with open(os.path.join(filedir, 'ae_result_columns.json'), 'r') as f:
		AE_RESULT_COLUMNS = json.load(f)

	with open(os.path.join(filedir, 'as_result_columns.json'), 'r') as f:
		AS_RESULT_COLUMNS = json.load(f)

	with open(os.path.join(filedir, 'pe_result_columns.json'), 'r') as f:
		PE_RESULT_COLUMNS = json.load(f)

	with open(os.path.join(filedir, 'ps_result_columns.json'), 'r') as f:
		PS_RESULT_COLUMNS = json.load(f)

	WAIT_TIME = 0.5 # Depending on the plan with PDL.

	def __init__(
		self,
		api_key: str,
		check_existing: bool=True,
		client_path: Optional[str] = None,
		max_workers: int = 30,
		sandbox: bool = True,
		aws_access_key_id: Optional[str] = None,
		aws_secret_access_key: Optional[str] = None,
		s3_bucket_name: Optional[str] = None,
		s3_ae_setup: bool = True,
		s3_pe_setup: bool = True,
		s3_ps_setup: bool = True,
		s3_as_setup: bool = False,
		reprocess_dataframes: bool = False,
	) -> None:
		'''
		Args:
			- api_key (str): People Data Labs api_key

			- check_existing_method (bool, default=True): 's3' (DB excluded for now)

			- client_path (str, default=None): Client's path to save pairs and
				identify which records belong to each client.

			- max_workers (int, default=30): Max number of workers to use in ThreadPoolExecutor

			- sandbox (bool, default=False): Whether to use sandbox or not.

			- aws_access_key_id (str, default=None): AWS access key id

			- aws_secret_access_key (str, default=None): AWS secret access key

			- s3_ae_setup (bool, default=True): Whether to setup s3 for AE or not.

			- s3_bucket_name (str, default=None): S3 bucket name to use.

			- s3_pe_setup (bool, default=True): Whether to setup s3 for PE or not.

			- s3_ps_setup (bool, default=True): Whether to setup s3 for PS or not.

			- s3_as_setup (bool, default=True): Whether to setup s3 for AS or not.

			- **kwargs: Additional kwargs to pass to s3_setup method.

		Returns:
			- None
		'''

		self.api_key = api_key

		self.base_url = self.SANDOX_URL if sandbox else self.BASE_URL

		self.check_existing = check_existing

		self.client_path = client_path

		self.max_workers = max_workers

		self.reprocess_dataframes = reprocess_dataframes

		self.s3_init(
			bucket_name=s3_bucket_name,
			aws_access_key_id=aws_access_key_id,
			aws_secret_access_key=aws_secret_access_key,
			ae_setup=s3_ae_setup,
			pe_setup=s3_pe_setup,
			ps_setup=s3_ps_setup,
			as_setup=s3_as_setup,
			reuse=False,
		)

		self.ae_pairs

	@TimeIt()
	def s3_init(
		self,
		bucket_name: Optional[str] = None,
		aws_access_key_id: Optional[str] = None,
		aws_secret_access_key: Optional[str] = None,
		ae_setup: bool = True,
		pe_setup: bool = True,
		ps_setup: bool = True,
		as_setup: bool = True,
		reuse: bool = True,
	):
		if self.check_existing is False:
			return

		if reuse:
			bucket_name = self.s3_init_params['bucket_name']
			aws_access_key_id = self.s3_init_params['aws_access_key_id']
			aws_secret_access_key = self.s3_init_params['aws_secret_access_key']
			ae_setup = self.s3_init_params['ae_setup']
			pe_setup = self.s3_init_params['pe_setup']
			ps_setup = self.s3_init_params['ps_setup']
			as_setup = self.s3_init_params['as_setup']

		else:
			self.s3_init_params = {
				'bucket_name': bucket_name,
				'aws_access_key_id': aws_access_key_id,
				'aws_secret_access_key': aws_secret_access_key,
				'ae_setup': ae_setup,
				'pe_setup': pe_setup,
				'ps_setup': ps_setup,
				'as_setup': as_setup,
			}

		s3_payload = {
			'service_name': 's3',
			'aws_access_key_id': aws_access_key_id,
			'aws_secret_access_key': aws_secret_access_key,
		}

		self.s3_resource = boto3.resource(**s3_payload)
		self.s3_client = boto3.client(**s3_payload)

		self.bucket_name = bucket_name
		self.bucket = self.s3_resource.Bucket(self.bucket_name)

		self.s3_folders = {
			's3_ae': 'account_enrichment' if ae_setup else None,
			's3_as': 'account_search' if as_setup else None,
			's3_pe': 'person_enrichment' if pe_setup else None,
			's3_ps': 'person_search' if ps_setup else None,
		}
		self.s3_folders = {i: j for i, j in self.s3_folders.items() if j}

		for key, value in self.s3_folders.items():
			print(f"Starting: {value} setup")

			filtered_files = self.bucket.objects.filter(Prefix=f"{value}/").all()
			filtered_files = [f for f in filtered_files if f.key != f"{value}/"]

			# [SPEEDUP PART 1] Using existing dataframes for speedup
			if self.reprocess_dataframes:
				existing_df = pd.DataFrame()

			else:
				df_files = self.bucket.objects.filter(Prefix=f"dataframes/{value}.csv")

				if len(list(df_files)) == 0:
					existing_df = pd.DataFrame()
				else:
					df_file = list(df_files)[0].get()['Body'].read().decode('UTF-8')

					existing_df = pd.read_csv(StringIO(df_file))

					filtered_files = [
						f for f in filtered_files if f.key.split('/')[-1].replace('.json', '') \
						not in existing_df['id'].values
					]
			###

			self.n = len(filtered_files)
			self.i = 0

			with ThreadPoolExecutor(max_workers=min(self.max_workers, len(filtered_files) or 1)) as ex:
				results = ex.map(self._read_file_from_s3, filtered_files)
				results = [*results]

			self.i = 0

			# dfs = [df for df in results if df is not None] 
			dfs = [existing_df] + [df for df in results if df is not None] 

			if dfs:
				joined_df = pd.concat(dfs, axis=0, ignore_index=True)

			else:
				joined_df = pd.DataFrame(
					columns=getattr(self, f"{key.replace('s3_', '').upper()}_RESULT_COLUMNS")
				)

			setattr(self, key, joined_df)

			# [SPEEDUP PART 2] Saving dataframes for speedup
			fmt_file = BytesIO(joined_df.to_csv(index=False).encode('UTF-8'))
			fmt_filename = f"dataframes/{value}.csv"
			###

			self.s3_client.upload_fileobj(fmt_file, self.bucket_name, fmt_filename)		

		print('Finished: s3_init')

	def _read_file_from_s3(self, file, verbose=True):
		try:
			fmt_file = file.get()['Body'].read().decode('UTF-8')
			df = pd.json_normalize(json.loads(fmt_file))

			if verbose:
				print(f'Finishing: {self.i}/{self.n}', end='\r')
				self.i += 1

			return df

		except Exception as e:
			if verbose:
				print(e)
				print(f"error: {file.key}")

			self.i += 1

	### Setting up client's pairs
	def _pairs(self, path: str, open_file: bool = False) -> Union[pd.DataFrame, None]:
		'''
		Internal method to return pairs 
		'''
		if not self.client_path:
			return None

		resp = None

		lst = self.bucket.objects.filter(Prefix=f"{path}/{self.client_path}").all()
		lst = list(lst)

		if open_file is False:
			if len(lst) == 0:
				return None

			lst = [i.key.split(self.client_path)[1].replace('.json', '') for i in lst]
			lst = [i.split('__')[1:] for i in lst]
			df = pd.DataFrame(lst, columns=['index', 'pdl_id'])
			return df

		self.n = len(lst)
		self.i = 0

		with ThreadPoolExecutor(max_workers=min(self.max_workers, len(lst) or 1)) as ex:
			results = ex.map(self._read_file_from_s3, lst)
		dfs = [df for df in results if df is not None]

		dfs = [df for df in dfs if not df.empty]

		# review this
		fmt = path.split('_')
		fmt = [fmt[0][0], fmt[1][0], '_', fmt[2]]
		fmt = ''.join(fmt)
		##

		if len(dfs) > 0:

			df = pd.concat(dfs, ignore_index=True)
			df.sort_values('source', inplace=True, ascending=False)
			df.drop_duplicates(subset=['index'], inplace=True)

			setattr(self, f'{fmt}_static', df.copy())

			return df

		setattr(self, f'{fmt}_static', None)
		return resp
			
	@property
	def ae_pairs(self):
		return self._pairs(path='account_enrich_pairs', open_file=True)

	@property
	def as_pairs(self):
		return self._pairs(path='account_search_pairs', open_file=True)

	@property
	def pe_pairs(self):
		return self._pairs(path='person_enriched_pairs') # Badly named in S3
		
	@property
	def ps_pairs(self):
		return self._pairs(path='person_search_pairs')

	### Dataframes associated with the client
	def _s3_df(self, path):
		df = getattr(self, f's3_{path}').copy()

		pairs_name = f'{path}_pairs'

		# if path.startswith('a'):
		# 	pairs_name += '_static'

		df_pairs = getattr(self, pairs_name)

		if df_pairs is None:
			return

		df = pd.merge(
			df,
			df_pairs[['index', 'pdl_id']],
			left_on='id',
			right_on='pdl_id',
			how='right'
		)
		df.drop_duplicates(subset=['index', 'pdl_id'], inplace=True)
		return df

	@property
	def s3_ae_client(self):
		return self._s3_df(path='ae')

	@property
	def s3_ps_client(self):
		return self._s3_df(path='ps')

	@property
	def s3_pe_client(self):
		return self._s3_df(path='pe')

	@property
	def s3_as_client(self):
		return self._s3_df(path='as')
	#########	

	def enrich_account(
		self,
		min_likelihood: int = 3,
		required: Optional[List[str]] = None,
		save: bool = True,
		check_existing: bool = True,
		s3_recalculate: bool = True,
		index: Optional[str] = None,
		return_response: bool = False,
		**kwargs
	):
		'''
		Args:
			 - min_likelihood (int, default=5): Minimum likelihood to consider a match

			 - required (list, default=None): List of required fields to consider a match

			 - save (bool, default=True): Save the results to S3

			 - check_existing (bool, default=True): Check if the results already exist in S3

			 - s3_recalculate (bool, default=True): Recalculate the results.
				
			 - index (str, default=None): Index to use for self.ae_pairs

			 - return_response (bool, default=False): Return the response from the API

		Returns:
			
			- pd.DataFrame: Dataframe with the results
		'''

		# Cleaning kwargs
		kwargs = {k: v for k, v in kwargs.items() if v not in [
			None, '', 'None', 'none', 'NONE', np.nan
		]}

		# Checking minimum fields.
		required_fields = {
			'name': ['name'],
			'website': ['website'],
			# 'profile': ['linkedin_url', 'facebook_url', 'twitter_url'],
			# 'ticker': ['ticker'],
		}

		# if not all(i in kwargs for i in required_fields):
		if not any(i in kwargs for i in required_fields):
			raise ValueError(f'You need to specify all of of {required_fields}')

		### STEP 1: Checking for a valid domain/website
		if 'website' in kwargs:
			raw_website = kwargs['website']
			kwargs['website'] = extract_domain(raw_website)

			if kwargs['website'] is None:
				print(f'Not a valid domain. {raw_website}')
				response = {
					'index': index,
					'pdl_id': None,
					'source': None
				}
				return response

		### STEP 2: Check if account exists according to INDEX.
		if check_existing and self.ae_pairs_static is not None and \
			index in self.ae_pairs_static['index'].values:

			response = {
				'index': index,
				'pdl_id': self.ae_pairs_static[
					self.ae_pairs_static['index'] == index
				]['pdl_id'].values[0],
				'source': 's3'
			}

			if return_response:
				data = self.s3_ae[self.s3_ae['id'] == response['pdl_id']]
				response['data'] = data.to_dict(orient='records')[0]

			return response

		### STEP 3: Check if account exists according to self.s3_ae
		if check_existing and self.check_existing is True:
			if hasattr(self, 's3_ae'):

				for key, value in required_fields.items():

					if key in kwargs:

						for v in value:

							if kwargs[key].lower() in self.s3_ae[v].values:

								data = self.s3_ae[
									self.s3_ae[v] == kwargs[key].lower()
								].to_dict('records')

								response = {
									'index': index,
									'pdl_id':data[0]['id'],
									'source': 's3'
								
								}

								if return_response:
									response['data'] = data[0]

								return response

		### STEP 4: Hit the API
		url = f"{self.base_url}/company/enrich"

		params = {
			"api_key": self.api_key,
			"min_likelihood": min_likelihood,
			"required": required
		}
		params.update(kwargs)

		json_response = requests.get(url, params=params).json()

		source = None

		if json_response["status"] == 200:
			source = 'api'

			if save and self.check_existing is True:
				fmt_filename = f"{self.s3_folders['s3_ae']}/{json_response['id']}.json"
				fmt_file = BytesIO(json.dumps(json_response).encode('UTF-8'))

				self.s3_client.upload_fileobj(fmt_file, self.bucket_name, fmt_filename)		
				
				if s3_recalculate:
					self.s3_init()

		result = {
			'index': index,
			'pdl_id': json_response.get('id'),
			'source': source
		}

		if return_response:
			result['data'] = json_response if json_response['status'] == 200 else None

		return result

	def bulk_enrich_account(
		self,
		account_list: list or pd.DataFrame,
		min_likelihood: int=5,
		required=None,
		save=True,
		check_existing=True,
		index_field: str=None,
		return_as_df: bool=True,
	):
		'''
		Args:
			
			- account_list (list or pd.DataFrame): List of accounts to enrich

			- min_likelihood (int, default=5): Minimum likelihood to consider a match

			- required (list, default=None): List of required fields

			- save (bool, default=True): Save the results to S3

			- check_existing (bool, default=True): Check if the account already exists

			- index_field (str, default=None): Field to use for self.ae_pairs

			- return_as_df (bool, default=True): Return the results as a dataframe

		Returns:

			- pd.DataFrame: Dataframe with the results
		'''

		if isinstance(account_list, pd.DataFrame):
			account_list = account_list.to_dict('records')

		for payload in account_list:
			payload.update({
				'min_likelihood': min_likelihood,
				'required': required,
				'save': save,
				'check_existing': check_existing,
				's3_recalculate': False,
				'return_response': False,
				'index': payload.get(index_field)
			})

		results = []

		i = 0

		for payload in account_list:
			print(
				'Processing: ',
				payload['index'],
				f"({i}/{len(account_list)})",
				end='\r'
			)

			r = self.enrich_account(**payload)
			results.append(r)

			i += 1

		if self.client_path:

			filtered_results = [r for r in results if r['pdl_id'] is not None]

			self.s3_client.upload_fileobj(
				BytesIO(json.dumps(filtered_results).encode('UTF-8')),
				self.bucket_name,
				f"account_enrich_pairs/{self.client_path}_{datetime.now()}.json",
			)

		self.s3_init()

		if return_as_df:
			results =  pd.DataFrame(results)

		return results

	def search_person(
		self,
		website: str,
		company_name: Optional[str] = None,
		required: str = 'work_email',
		strategy: str = 'AND',
		check_existing: bool = True,
		return_size: int=  1,
		save: bool = True,
		s3_recalculate: bool = False,
		verbose: bool = True,
		index: Optional[str] = None,
		**kwargs
	) -> Dict:
		'''
		Args:
			
			- company_name (str): Company name

			- website (str): Company website

			- required (str, default='work_email'): Required field

			- strategy (str, default='AND'): Search strategy

			- check_existing (bool, default=True): Check if the person already exists

			- return_size (int, default=1): Number of results to return

			- save (bool, default=True): Save the results to S3

			- s3_recalculate (bool, default=True): Recalculate the S3 dataframes

			- verbose (bool, default=True): Print the query

			- index (str, default=None): Index to use for self.ps_pairs

			- **kwargs: Additional arguments to pass to the API

		Returns:
			
			- dict: Dictionary with the results
		'''

		# Check valid parameters 
		for i, _ in kwargs.items():
			if i not in type(self).PS_RESULT_COLUMNS:
				raise ValueError(f"{i} not valid. Check PeopleDataLabs.PS_RESULT_COLUMNS")

		if company_name:
			kwargs['job_company_name'] = company_name.lower()

		website = extract_domain(website).lower() if extract_domain(website) else None
		kwargs['job_company_website'] = website 

		url = f"{self.base_url}/person/search"

		H = {
		  'Content-Type': "application/json",
		  'X-api-key': self.api_key
		}

		# SQL query construction
		sql = f"SELECT * FROM person WHERE"

		if kwargs:
			where_str_list = []

			for k, v in kwargs.items():

				if isinstance(v, list):
					v_fmt = ', '.join([f"'{i}'" for i in v])
					kv_fmt = f"{k} IN ({v_fmt})" 

				else:
					# kv_fmt = f"{k} LIKE '%{v}%'"
					kv_fmt = f"{k} = '{v}'"

				where_str_list.append(kv_fmt)

			where_str = f' {strategy} '.join(where_str_list)

			sql += ' '
			sql += where_str

		if required:
			if kwargs:
				sql += ' AND'
			sql += f' {required} IS NOT NULL'

		if check_existing and self.check_existing is True:
			if hasattr(self, 's3_ps') and self.s3_ps.shape[0] > 0:

				if company_name:
					existing = self.s3_ps.loc[
						self.s3_ps['job_company_name'].str.lower().str.contains(company_name.lower()),
						['full_name']
					]
				else:
					existing = self.s3_ps.loc[
						self.s3_ps['job_company_website'] == website,
						['full_name']
					]

				if not existing.empty:
					existing = tuple(existing['full_name'])

					if len(existing) == 1:
						existing = str(existing).replace(",", "")

					existing_str = f" AND full_name NOT IN {existing}"

					sql += existing_str

		if verbose:
			print(sql)

		P = {
		  'sql': sql,
		  'size': return_size,
		  'pretty': True
		}

		response = requests.get(
		  url,
		  headers=H,
		  params=P
		).json()

		if response['status'] == 200:
			for person in response['data']:
				id = person['id']

				if save and self.check_existing is True:
					
					fmt_filename = f"{self.s3_folders['s3_ps']}/{id}.json"

					fmt_file = BytesIO(json.dumps(person).encode('UTF-8'))

					self.s3_client.upload_fileobj(
						fmt_file,
						self.bucket_name,
						fmt_filename
					)		

					self.s3_client.upload_fileobj(
						BytesIO(json.dumps('').encode('UTF-8')),
						self.bucket_name,
						f"person_search_pairs/{self.client_path}__{index}__{id}.json"
					)		

			if s3_recalculate:
				self.s3_init()

		return response

	def enrich_person(
		self,
		linkedin_url: str,
		save: bool = True,
		check_existing: bool = True,
		s3_recalculate: bool = False,
		index: Optional[str] = None
	) -> Dict:
		if check_existing and linkedin_url in self.s3_pe.linkedin_url.tolist():
			print(f"Person already enriched: {linkedin_url}")
			response = self.s3_pe[
				self.s3_pe.linkedin_url == linkedin_url
			].iloc[0].to_dict()
			# Pending associate index if not in self.s3_pe_client
			return response

		url = f"{self.base_url}/person/enrich"
		params = {
			"api_key": self.api_key,
			"min_likelihood": 5,
			"profile": linkedin_url
		}
		response = requests.get(url, params=params).json()
		if response['status'] == 200:
			if save:
				self.s3_client.upload_fileobj(
					BytesIO(json.dumps(response['data']).encode('UTF-8')),
					self.bucket_name,
					f"{self.s3_folders['s3_pe']}/{response['data']['id']}.json",
				)
				self.s3_client.upload_fileobj(
					BytesIO(json.dumps('').encode('UTF-8')),
					self.bucket_name,
					f"person_enriched_pairs/{self.client_path}__{index}__{response['data']['id']}.json"
				)		
		if s3_recalculate:
			self.s3_init()
		return response['data']

	def bulk_search_person(
		self,
		account_list: Union[list, pd.DataFrame],
		required: str = 'work_email',
		verbose: bool = False,
		return_size: int = 1,
		enrich: bool = False
	):
		'''
		Args:
			- account_list (list or pd.DataFrame): List of accounts to search

			- required (str, default='work_email'): Required field

			- verbose (bool, default=False): Print the query

			- return_size (int, default=1): Number of results to return

		Returns:
			- dict: Dictionary with the results
		'''
		if isinstance(account_list, pd.DataFrame):
			account_list = account_list.to_dict('records')

		results = []

		for payload in account_list:

			print(f"Processing {payload['company_name']}")

			payload.update({
				'required': required,
				'verbose': verbose,
				'return_size': return_size,
				's3_recalculate': False,
				'enrich': enrich,
			})

			resp = self.search_person(**payload)

			results.append(resp)

			print(f"Processed | {resp['status']} | {payload['company_name']}")

			time.sleep(type(self).WAIT_TIME)

		self.s3_init(**self.s3_init_params)

		return results
