from typing import List

import pandas as pd

from .main import SalesforceObj
from ..utils.entity_resolution import Matcher


class DataLoader:

	ALLOWED_SOBJECTS = [
		'Account',
		'Contact',
	]

	def __init__(self, config):
		self.loaded_compare_data = False

		self.sf = SalesforceObj(**config)

		for sobject in self.ALLOWED_SOBJECTS:
			setattr(self, f"set_{sobject}", False)

		self.matcher = Matcher(exact_domain=False)

	def load_account_compare_data(
		self,
		data: pd.DataFrame=None,
		name: str='compare',
		index='Id',
		domain='Website',
		address='BillingStreet',
		phone='Phone',
		country='BillingCountry',
		entity_name='Name'
	):
		print('Starting to load account compare data')
		# If a dataframe is not provided, we load it from Salesforce.
		if data is None:
			data = self.sf.query_parallel(
				tablename='Account',
				columns=[
					index, domain, address, phone, country, entity_name
				],
				limit=None,
				df=True,
				date_window=None,
				date_window_variable='LastModifiedDate',
				verbose=False,
				include_deleted=False,
				n_chunks=5
			)

		self.matcher.set_df(
			data=data,
			name=name,
			index=index,
			domain=domain,
			address=address,
			phone=phone,
			country=country,
			entity_name=entity_name
		)

		self.loaded_compare_data = True

		self.compare_index = index

		self.compare_data = data

	def load_data(
		self,
		path_or_dataframe: str or pd.DataFrame,
		sobject: str='Account',
		sobject_join_column: str or list=None,
	):
		if sobject not in self.ALLOWED_SOBJECTS:
			raise ValueError(
				f'{sobject} is not an allowed Salesforce object. Choose one of {self.ALLOWED_SOBJECTS}'
			)

		if type(path_or_dataframe) == pd.DataFrame:
			data = path_or_dataframe

		else:
			data = pd.read_csv(filepath_or_buffer=path_or_dataframe)

		data = data.copy()

		# When sobject != 'Account', format joiner column and exclude for
		# map types
		joiner = False
		if sobject_join_column in data.columns and sobject != 'Account':

			joiner_series = data[sobject_join_column]

			data.drop(columns=[sobject_join_column], inplace=True)

			setattr(self, f"{sobject}_join_column", f"{sobject_join_column}_join_column")

			joiner = True

		data = self.sf.map_types(
			df=data,
			tablename=sobject,
			check_column_casing=True,
			return_as_dict=False
		)

		if joiner:
			data[f"{sobject_join_column}_join_column"] = joiner_series

		setattr(self, f"set_{sobject}", True)

		setattr(self, f"{sobject}", data)

	def to_sf(
		self,
		tablename: str,
		dataframe: pd.DataFrame,
		update=True,
		insert=True,
		conflict_on:str or List[str]='Id',
		return_response: bool=True,
		overwrite: bool=False,
		overwrite_columns: str or List[str]=None,
		verbose: bool=False,
		update_diff_on: list=None,
	):
		resp = self.sf.upsert_df(
			tablename=tablename,
			dataframe=dataframe,
			update=update,
			insert=insert,
			conflict_on=conflict_on,
			return_response=return_response,
			overwrite=overwrite,
			overwrite_columns=overwrite_columns,
			update_diff_on=update_diff_on,
		)
		resp_fmt = {i: j for i, j in resp.items()}

		if verbose is False:
			for k, v in resp.items():
				resp_fmt[k] = {i: j for i, j in v.items() if i != 'result'}

		print(f"{tablename}:\n{resp_fmt}")

		return resp

	def main(
		self,
		account_conflict_on: str='Name',
		contact_conflict_on: str or list=['FirstName', 'LastName', 'Email'],
		create_accounts: bool=True,
		update_accounts: bool=True,
		create_contacts: bool=True,
		update_contacts: bool=True,
		verbose=False,
		update_only_missing_on: list=None
	):
		'''
		Args:
			- account_conflict_on (str, default='Name'): conflict
				resolution on Account.

			- contact_conflict_on (str | List(str), default=['FirstName', 'LastName', 'Email']):
				conflict resolution on Contact.

			- create_accounts (bool, default=True):

			- update_accounts (bool, default=True):

			- create_contacts (bool, default=True):

			- update_contacts (bool, default=True):

			- verbose (bool, default=False): Include result in self.to_sf response. 

			- update_only_missing_on (List(str), default=None):
				If not None, only update records where there is a 
				difference in these columns. It saves api calls.

		'''
		if self.set_Account is False:
			raise ValueError('Account data has not been loaded')


		# Load compare data if not already loaded: Matcher part 1
		if not self.loaded_compare_data and account_conflict_on != 'Id':
			self.load_account_compare_data()

		self.Account.dropna(subset=[account_conflict_on], inplace=True)

		ac = self.Account.columns

		if account_conflict_on != 'Id':
			# Matcher part 2
			self.matcher.set_df(
				data=self.Account,
				name='new',
				index=account_conflict_on,
				domain='Website' if 'Website' in ac else None,
				address='BillingStreet' if 'BillingStreet' in ac else None,
				phone='Phone' if 'Phone' in ac else None,
				country='BillingCountry' if 'BillingCountry' in ac else None,
				entity_name='Name' if 'Name' in ac else None
			)

			matches = self.matcher.run(
				match_type_included = [   
					['domain'],
					['domain', 'entity_name'],
					['domain', 'address'],
					['domain', 'phone'],
					['entity_name', 'address'],
					['entity_name', 'phone'],

					['entity_name'],
				]
				
			)
			matches_mapping = matches[[
				f"{account_conflict_on}_new", f"{self.compare_index}_compare"
			]].groupby(f"{account_conflict_on}_new").first()[
				f"{self.compare_index}_compare"
			]
			
			# Obtaining AccountId for existing accounts
			self.Account['Id'] = self.Account[account_conflict_on].map(matches_mapping)

		existing_accounts = self.Account[self.Account['Id'].notnull()]
		existing_accounts.drop_duplicates(subset=['Id'], inplace=True)

		new_accounts = self.Account[self.Account['Id'].isnull()]
		new_accounts.drop(columns=['Id'], inplace=True, axis=1)

		print(f'Existing accounts: {len(existing_accounts)}')
		print(f'New accounts: {len(new_accounts)}')

		new_accounts_resp = {'insert': {}}
		if create_accounts and len(new_accounts) > 0:
			new_accounts_resp = self.to_sf(
				tablename='Account',
				dataframe=new_accounts,
				update=False,
				insert=True,
				conflict_on=account_conflict_on,
				return_response=True,
				overwrite=False,
				overwrite_columns=None,
				verbose=verbose
			)

		existing_accounts_resp = {'update': {}}
		if update_accounts and len(existing_accounts) > 0:
			existing_accounts_resp = self.to_sf(
				tablename='Account',
				dataframe=existing_accounts,
				update=True,
				insert=False,
				conflict_on=list(set(['Id'] + [account_conflict_on])),
				# conflict_on='Id',
				return_response=True,
				overwrite=False,
				overwrite_columns=None,
				verbose=verbose,
				update_diff_on=update_only_missing_on,
			)

			# if existing_accounts_resp:
			#
			# 	print(existing_accounts[account_conflict_on])
			# 	print(existing_accounts_resp['update']['result'])
			#
			# 	for v, result  in zip(
			# 		existing_accounts[account_conflict_on].values,
			# 		existing_accounts_resp['update']['result']
			# 	):
			# 		result[account_conflict_on] = v


		accountid_mapping = new_accounts_resp.get('insert', {}).get('result', []) + \
			existing_accounts_resp.get('update', {}).get('result', [])

		accountid_mapping = pd.DataFrame(accountid_mapping)

		if account_conflict_on != 'Id':
			accountid_mapping = accountid_mapping[['id'] +  [account_conflict_on]]
			accountid_mapping.dropna(subset=['id'], inplace=True)

		else:
			accountid_mapping = accountid_mapping[['id']]
			accountid_mapping.dropna(subset=['id'], inplace=True)
			accountid_mapping[account_conflict_on] = accountid_mapping['id']

		### CONTACT PROCESSING ###
		if self.set_Contact:

			self.Contact = pd.merge(
				self.Contact,
				accountid_mapping,
				how='inner',
				left_on=self.Contact_join_column,
				right_on=account_conflict_on,
			)

			if not self.Contact.empty:

				self.Contact.drop(self.Contact_join_column, inplace=True, axis=1)

				self.Contact.rename(columns={'id': 'AccountId'}, inplace=True)

				if account_conflict_on in self.Contact.columns:
					self.Contact.drop(
						columns=[account_conflict_on], inplace=True, axis=1
					)

				self.Contact.dropna(subset=contact_conflict_on, inplace=True)

				self.to_sf(
					tablename='Contact',
					dataframe=self.Contact,
					update=update_contacts,
					insert=create_contacts,
					conflict_on=contact_conflict_on,
					return_response=True,
					overwrite=False,
					overwrite_columns=None,
					verbose=verbose
				)
			else:
				print('No Contact data to upsert.')















