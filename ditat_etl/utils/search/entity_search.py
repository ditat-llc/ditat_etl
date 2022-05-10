import os

import pandas as pd
import numpy as np
import spacy

from ...time import TimeIt


class EntitySearch:
	def __init__(self, path_or_dataframe, th=0.2):
		self.nlp_th = th

		self.load_nlp()
		self.load_df(path_or_dataframe)

		self.deterministic_vars = [
			'city', 'country', 'state'
		]

	def load_df(self, path_or_dataframe):
		if isinstance(path_or_dataframe, str):
			self.df = pd.read_csv(path_or_dataframe)
		else:
			self.df = path_or_dataframe

		self.numeric_cols = self.df.select_dtypes(include=[np.number]).columns
		self.object_cols = self.df.select_dtypes(exclude=[np.number]).columns

	@TimeIt()
	def load_nlp(self, english_pipeline='en_core_web_lg', th=0.4):
		try:
			self.nlp = spacy.load(english_pipeline)

		except Exception:
			os.system(f"python -m spacy download {english_pipeline}")
			self.nlp = spacy.load(english_pipeline)

	def _npl_search(self, series, values):
		values_nlp = [self.nlp(i) for i in values]

		series_unique = [i for i in series.unique() if isinstance(i, str)]

		mapping = {i: self.nlp(i) for i in series_unique}

		nlp_series = series.apply(lambda x: mapping[x] if x in mapping else None)

		result = nlp_series.apply(
			lambda x: max([x.similarity(i) for i in values_nlp]) if x else 0
		)

		return result

	@TimeIt()
	def run(self, **kwargs):
		'''
		Run the entity search on the dataframe (self.df)

		Args:
			**kwargs (dict): keyword arguments to pass to the entity search

		Returns:
			A dataframe with the results of the entity search

		The process is divided into 3 types of searches:
			1. Numeric columns
			2. Object columnn without NPL (cities, countries)
			3. Object columns with NPL (industry, titles)

		'''
		df = self.df.copy()

		# Variables setup
		numeric_vars = {
			i: j for i, j in kwargs.items() if i in self.numeric_cols
		}
		object_vars = {i: j for i, j in kwargs.items() if i in self.object_cols}

		# No NLP vars
		no_npl_vars = []

		for i in object_vars:
			for j in self.deterministic_vars:
				if j in i:
					no_npl_vars.append(i)

		npl_vars = [i for i in object_vars if i not in no_npl_vars]
		
		# Process
		for k, v in object_vars.items():
			object_vars[k] = v if isinstance(v, list) else [v]
			object_vars[k] = [i.lower() for i in object_vars[k]]


		df[self.object_cols] = df[self.object_cols].apply(lambda x: x.str.lower())
		
		# Numeric filtering
		for k, v in numeric_vars.items():
			a = v[0]
			b = v[1]

			if a:
				df = df[df[k] >= a]

			if b:
				df = df[df[k] <= b]

		# No npl filtering
		for k, v in object_vars.items():
			if k in no_npl_vars:
				df= df[df[k].str.contains("|".join(v), na=False)]

		# NLP filtering
		for k, v in object_vars.items():
			if k in npl_vars:
				df[f"nlp_sim_{k}"] = self._npl_search(df[k], v)

				df = df[df[f"nlp_sim_{k}"] >= self.nlp_th]

		nlp_cols = [c for c in df.columns if c.startswith('nlp_sim')]

		df['nlp_total'] = df[nlp_cols].mean(axis=1)

		df.sort_values(by='nlp_total', ascending=False, inplace=True)

		return self.df.loc[df.index]

	def distribute(self,
		payload: dict,
		size: int=10,
		repetition: bool=False
	):
		'''
		Distribute the payload to the different searches

		Args:
			payload (dict): {
				"john": {"industry": ["software", "hardware"]}},
				"mary": {"industry": ["software", "hardware"]}

		Returns:
		    resp (dict): {"john": pd.DataFrame, "mary": [...]}
	
		'''

		resp = {i: self.run(**j) for i, j in payload.items()}

		if not repetition:
			used = []

			for name, df in resp.items():
				resp[name] = df.loc[~df.index.isin(used)].head(size)
				used.extend(resp[name].index)

		resp = {i: df.head(size) for i, df in resp.items()}

		return resp







