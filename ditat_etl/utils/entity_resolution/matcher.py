import re
import json
import os
from functools import wraps

import pandas as pd
import numpy as np

from ..phones import Phone
from ...url.functions import extract_domain


filedir = os.path.abspath(os.path.dirname(__file__))


class Frame:
	def __init__(
		self,
		data: pd.DataFrame,
		name: str,
		index='id',
		domain=None,
		address=None,
		phone=None,
		country=None,
		entity_name=None,
		):
		self.data = data.copy()
		self.name = name

		attributes = [
			"index", "domain", "address",
			"phone", "country", "entity_name",
		]
		for attr in attributes:
			setattr(self, attr, f"{locals()[attr]}_{name}" if locals()[attr] else None)
		
		self.suffix = f"_{name}"
		self.data = self.data.add_suffix(f'_{name}')

class Matcher:
	def __init__(self, exact_domain=False):
		self._counter = 1
		self._names = []
		self.exact_domain = exact_domain
		self.ignored_domains = self.load_ignored_domains()
		self.features_candidates = [
			"domain", "phone", "address",
			"entity_name",
		]
		self.features_processed = []

	def load_ignored_domains(self, path=os.path.join(filedir, 'domains_ignored.txt')):
		with open(path, 'r') as f:
			result = f.read().splitlines()
		return result

	def set_df(
		self,
		data: pd.DataFrame,
		name: str,
		index='id',
		domain=None,
		address=None,
		phone=None,
		country=None,
		entity_name: str=None
		):
		'''
		Args:
			- data (pd.Dataframe)
			- name (str): Name of dataframe used for suffixes 
			- index (str, default='id')
			- domain (str, default=None)
			- address (str, default=None)
			- phone (str, default=None)
			- country (str, default=None)
			- entity_name (str, default=None): Company/Person Name,
				do not confuse with name

		Note:
			- Since the matching might either have One to One relationship
				or Many to One relationship, you have to set the dataframe with Many first.
		'''
		if self._counter > 2:
			raise ValidationError('You have already set both dataframes.')

		if name in self._names:
			raise ValueError('Name already taken!')

		frame = Frame(
			data=data,
			name=name,
			index=index,
			domain=domain,
			address=address,
			phone=phone,
			country=country,
			entity_name=entity_name
		)

		setattr(self, f"frame__{self._counter}", frame)
		
		self._counter += 1
		self._names.append(name)

	def dedupe(
		self,
		data: pd.DataFrame,
		index='id',
		domain=None,
		address=None,
		phone=None,
		country=None,
		entity_name: str=None,
		save=True,
		match_type_included=None,
		match_type_excluded=None,
		match_count_th=3,
		include_self=True
	):
		self.include_self = include_self

		vars = locals().copy()

		left = vars.copy()
		left['name'] = 'left'

		right = vars.copy()
		right['name'] = 'right'

		type(self).set_df(
			**{i: j for i, j in left.items() if i in self.set_df.__code__.co_varnames}
		)
		type(self).set_df(
			**{i: j for i, j in right.items() if i in self.set_df.__code__.co_varnames}
		)
		match_type_included = match_type_included or [   
			# ['domain'],
			# ['entity_name'],
			['domain', 'entity_name'],
			['domain', 'address'],
			['domain', 'phone'],
			['entity_name', 'address'],
			['entity_name', 'phone'],
		]

		self.run(
			save=save,
			deduping=True,
			match_type_included=match_type_included,
			match_type_excluded=match_type_excluded,
			match_count_th=match_count_th or 3,
		)

		results = self.results.copy()

		if results.shape[0] == 0:
			return None

		index_col = getattr(self.frame__1, 'index').replace('_left', '')

		def row_function(x, one, two):
			result = list(set([x[one], x[two]])) 

			# Domain
			if one == getattr(self.frame__1, 'domain'):
				result = [extract_domain(i) for i in result]

			result = [i for i in result if i not in (None, np.nan)]
			result = [i for i in result if str(i) != 'nan']
			return result

		for col in self.frame__1.data:

			column_name = col.replace('_left', '')
			one = col
			two = col.replace('_left', '_right')

			results[f"{column_name}__agg"] = results.apply(
				lambda x: row_function(x, one, two),
				axis=1
			) 

		def agg_function(row): 
			result = list(set(
				[item for sublist in row for item in sublist]
			))
			return result

		results = results.groupby('match_group')[[
			col for col in results if col.endswith('__agg')
		]].agg(agg_function)

		results.columns = [col.replace('__agg', '') for col in results.columns]

		results['temp'] =  results[index_col].str.len()

		results.sort_values(
			'temp',
			inplace=True,
			ascending=False
		)
		results.drop('temp', inplace=True, axis=1)

		if save:
			results.to_csv('dedupes.csv', index=False)

		print(f'Original dataframe after deduping: {results.shape[0]}')

		return results
	
	@staticmethod
	def clean_field(x):
		combo_replacements = [
			"'s", "s.a.", "p.c.", "n.a."
		]
		for r in combo_replacements:
			x = x.replace(r, " ")

		x = re.sub("([\(\[]).*?([\)\]])", "\g<1>\g<2>", x)

		replacements = [
			",", ".", '"', "'", "!", "?", "/", "-", "&", "#", "%",
			"@", "$", '^', "*", "(", ")", "+", "\\", ">", "<", "'s",
			"'S"
		]
		for r in replacements:
			x = x.replace(r, " ")

		x = re.sub('([A-Z][a-z]+)', r' \1', re.sub('([A-Z]+)', r' \1', x))

		x = x.lower()
		x = x.split()
		x = sorted(x)
		x = [i for i in x if len(i) >= 1]

		excluded_names = [
			'llc', 'inc', 'corp', 'co', 'ltd', 'and',
			'group', 'of', 'the', 'group', 'union',
			'company', 'ctr', 'sac', 'care', 'limited',
			'store', 'medical', 'lp', 'medical', 'service',
			'services', 'corps', 'lab', 'labs', 'incorporated',
			'fzc', 'design', 'designs', "srl", "club", "builder",
			"builders", "clothing", "sport", "sports", "residential",
			"logistic", "logistics", "pvt", "system", "systems", 
			"clubs", "industry", "industries", "specialist", "specialists",
			"system", "systems", "restaurant", "restaurants", "institute",
			"education", "center", "network" 

		]
		x = [i for i in x if i not in excluded_names]

		x = [i[:-1] if i[-1] == 's' and len(i) > 2 else i for i in x]

		if len(x) >= 1:
			return str(x)

	def generic(function):
		'''
			Generic decorator to avoid repeating the same logic for
			every feature we add. Each specific function is wrapped
			with this decorator and implents specific logic using:
			i. df_1 ii. df_2 iii. var

			If there is a condition not met inside the specific logic,
			we have to return False and the process is interrupted
		'''
		@wraps(function)
		def wrapper(self, verbose=False, *args, **kwargs):
			# Dynamically retrieving the function name.
			name = function.__name__
			var = f'{name}_fmt'

			# If either frame does not have the attribute {name}.
			if all([
				getattr(self.frame__1, name),
				getattr(self.frame__2, name),
			]):

				df_1 = self.frame__1.data.copy()
				df_2 = self.frame__2.data.copy()

				df_1.dropna(subset=[getattr(self.frame__1, name)], inplace=True)
				df_2.dropna(subset=[getattr(self.frame__2, name)], inplace=True)

				df_1[var] = df_1[getattr(self.frame__1, name)]
				df_2[var] = df_2[getattr(self.frame__2, name)]

				# Calling the specific function
				r = function(self, df_1, df_2, var, *args, **kwargs)
				if r is False:
					return None

				self.features_processed.append(name)

				df_1.dropna(subset=[var], inplace=True)
				df_2.dropna(subset=[var], inplace=True)

				matches = pd.merge(df_1, df_2, on=var, how='left',  suffixes=(self.frame__1.suffix, self.frame__2.suffix))
				matches = matches[~matches[self.frame__2.index].isnull()]

				matches = matches[[self.frame__1.index, self.frame__2.index, var]]

				if verbose:
					print(f'{name} matches: {matches.shape[0]}')

				matches['match_type'] = name

				return matches
		return wrapper

	@generic
	def entity_name(self, df_1, df_2, var):
		df_1[var] = df_1[var].apply(type(self).clean_field)
		df_2[var] = df_2[var].apply(type(self).clean_field)

	@generic
	def address(self, df_1, df_2, var):
		df_1[var] = df_1[var].apply(type(self).clean_field)
		df_2[var] = df_2[var].apply(type(self).clean_field)
	   
	@generic
	def domain(self, df_1, df_2, var):
		# if self.exact_domain:
		#	 df_1[var] = df_1[self.frame__1.domain]
		#	 df_2[var] = df_2[self.frame__2.domain]
		# else:
		df_1[var] = df_1[self.frame__1.domain].apply(
			extract_domain, isemail=self.exact_domain
		)
		df_2[var] = df_2[self.frame__2.domain].apply(
			extract_domain, isemail=self.exact_domain
		)

		df_1 = df_1[~df_1[var].isin(self.ignored_domains)]
		df_2 = df_2[~df_2[var].isin(self.ignored_domains)]

	@generic
	def phone(self, df_1, df_2, var):
		if self.frame__1.country is None or self.frame__2.country is None:
			return False

		df_1[var] = df_1.apply(lambda x: Phone.format(x[self.frame__1.phone], x[self.frame__1.country]), axis=1)
		df_2[var] = df_2.apply(lambda x: Phone.format(x[self.frame__2.phone], x[self.frame__2.country]), axis=1)

	def run(
		self,
		save=False,
		verbose=True,
		deduping=False,
		match_type_included=None,
		match_type_excluded=None,
		match_count_th=3
	):
		if not match_type_included:
			match_type_included = [   
				['domain'],
				['domain', 'entity_name'],
				['domain', 'address'],
				['domain', 'phone'],
				['entity_name', 'address'],
				['entity_name', 'phone'],
			]

		match_type_excluded = match_type_excluded or []

		match_type_excluded = [json.dumps(sorted(i)) for i in match_type_excluded]

		match_type_included = [json.dumps(sorted(i)) for i in match_type_included]
		match_type_included = [i for i in match_type_included if i not in match_type_excluded]

		# Run individual matches according to attribute in self.features_candidates and concatenate.
		for feature in self.features_candidates:
			setattr(
				self, f"{feature}_matches",
				getattr(self, feature)(verbose=verbose)
			)

		self.all_matches = pd.concat(
			[getattr(self, f"{i}_matches") for i in self.features_processed]
		)

		# Create dummy column.
		# One important thing here is to group by first by self.frame__2.index since it has the One to Many relationship
		self.all_matches = self.all_matches.groupby([self.frame__1.index, self.frame__2.index]).agg({'match_type': ['count', list]})
		columns = ['match_count', 'match_type']
		self.all_matches.columns = columns
		self.all_matches['match_type'] = self.all_matches['match_type'].apply(lambda x: json.dumps(sorted(x)))
		self.all_matches.sort_values(['match_count', 'match_type'], ascending=False, inplace=True)
		self.all_matches.reset_index(inplace=True)

		# Merge the original dataframes with the pivot 'self.all_matches'.
		results = pd.merge(self.frame__1.data, self.all_matches, on=self.frame__1.index)
		results = pd.merge(results, self.frame__2.data, on=self.frame__2.index, suffixes=(self.frame__1.suffix, self.frame__2.suffix))
		results.sort_values(['match_count', 'match_type'], ascending=False, inplace=True)

		# Handling the order of columns
		column_order_list = []
		for feature in self.features_processed:
			feature_name_frame__1 = getattr(self.frame__1, feature) 
			feature_name_frame__2 = getattr(self.frame__2, feature) 
			column_order_list.append(feature_name_frame__1)
			column_order_list.append(feature_name_frame__2)
		
		column_order_list.extend(columns) 

		for col in column_order_list:
			f_col = results.pop(col)
			results.insert(0, col, f_col)

		if deduping:

			def f(row):
				a = row[self.frame__1.index]
				b = row[self.frame__2.index]
				r = sorted([a, b])
				return str(r)

			results['temp']= results.apply(f, axis=1)

			results.drop_duplicates(subset=['match_type', 'temp'], inplace=True)

			results.drop('temp', axis=1, inplace=True)

			# We mark the matches of the same row as 99.
			if self.include_self is True:
				results.loc[results[self.frame__1.index] == results[self.frame__2.index], 'match_count'] = 99

			else:
				results = results[results[self.frame__1.index] != results[self.frame__2.index]]

		# Filtering according to match_type and match_count
		results = results.loc[ 
			(results.match_count >= match_count_th)
			| (results.match_type.isin(match_type_included)),
			:
		]

		# Match Grouping
		group_dict = {}
		counter = 0
		
		for _, row in results.iterrows():

			li = row[self.frame__1.index]
			ri = row[self.frame__2.index]

			if li in group_dict:
				group = group_dict[li]
				group_dict[ri] = group

			elif ri in group_dict:
				group = group_dict[ri]
				group_dict[li] = group

			else:
				group = counter
				group_dict[li] = group
				group_dict[ri] = group

				counter += 1

		results.insert(2, 'match_group', results[self.frame__1.index].map(group_dict))

		# Sorting according to value counts of match_group and match_count
		sorting = results.match_group.value_counts()
		results['temp_sort'] = results.match_group.map(sorting)

		results.sort_values(['temp_sort', 'match_group', 'match_count'], ascending=[False, True, False], inplace=True)
		results.drop('temp_sort', axis=1, inplace=True)

		print(f"Results filtered: {results.shape[0]}")

		print(f"N Unique rows: {results['match_group'].nunique()}")

		self.results = results

		if save:
			self.results.to_csv(f'matches.csv', index=False)

		return self.results
