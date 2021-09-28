import pandas as pd

from .phones import Phone
from ..url.functions import extract_domain


'''
Entity resolution matcher according to:
	- Email and domain
	- Phone
	- Address
'''

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
		suffix=''
		):
		self.data = data
		self.index = f"{index}{suffix}"
		self.suffix = suffix
		self.name = name
		self.domain = domain
		self.address = address
		self.phone = phone
		self.country = country

		self.data.rename(columns={index: self.index}, inplace=True)


class Matcher:
	def __init__(self):
		self._counter = 1

	def set_df(self, **kwargs):
		'''
		Args:
			- **kwargs: All paramters for Frame.__init__()

		Note:
			- Since the matching might either have One to One relationship
				or Many to One relationship, you have to set the dataframe with Many first.
		'''
		frame = Frame(suffix=f"__{self._counter}", **kwargs)
		setattr(self, f"frame{frame.suffix}", frame)
		self._counter += 1

	def phone(self, verbose=False):
		if all([
			self.frame__1.phone,
			self.frame__1.country,
			self.frame__2.phone,
			self.frame__2.country
		]):
			var = 'phone_fmt'

			df_1 = self.frame__1.data.copy()
			df_2 = self.frame__2.data.copy()

			df_1.dropna(subset=[self.frame__1.phone], inplace=True)
			df_2.dropna(subset=[self.frame__2.phone], inplace=True)

			df_1[var] = df_1.apply(lambda x: Phone.format(x[self.frame__1.phone], x[self.frame__1.country]), axis=1)
			df_2[var] = df_2.apply(lambda x: Phone.format(x[self.frame__2.phone], x[self.frame__2.country]), axis=1)

			df_1.dropna(subset=[var], inplace=True)
			df_2.dropna(subset=[var], inplace=True)

			matches = pd.merge(df_1, df_2, on=var, how='left',  suffixes=(self.frame__1.suffix, self.frame__2.suffix))
			matches = matches[~matches[self.frame__2.index].isnull()]

			matches = matches[[self.frame__1.index, self.frame__2.index]]
			if verbose:
				print(f'Phone matches: {matches.shape[0]}')
			return matches

	def domain(self, verbose=False):
		if all([
			self.frame__1.domain, 
			self.frame__2.domain
		]):
			var = 'domain_fmt'

			df_1 = self.frame__1.data.copy()
			df_2 = self.frame__2.data.copy()

			df_1.dropna(subset=[self.frame__1.domain], inplace=True)
			df_2.dropna(subset=[self.frame__2.domain], inplace=True)

			df_1[var] = df_1[self.frame__1.domain].apply(extract_domain)
			df_2[var] = df_2[self.frame__2.domain].apply(extract_domain)

			df_1.dropna(subset=[var], inplace=True)
			df_2.dropna(subset=[var], inplace=True)

			matches = pd.merge(df_1, df_2, on=var, how='left',  suffixes=(self.frame__1.suffix, self.frame__2.suffix))
			matches = matches[~matches[self.frame__2.index].isnull()]

			matches = matches[[self.frame__1.index, self.frame__2.index]]
			if verbose:
				print(f'Domain matches: {matches.shape[0]}')

			return matches

	def address(self, verbose=False):
		if all([
			self.frame__1.address, 
			self.frame__2.address
		]):
			var = 'address_fmt'

			df_1 = self.frame__1.data.copy()
			df_2 = self.frame__2.data.copy()

			df_1.dropna(subset=[self.frame__1.address], inplace=True)
			df_2.dropna(subset=[self.frame__2.address], inplace=True)

			df_1[var] = df_1[self.frame__1.address]
			df_2[var] = df_2[self.frame__2.address]

			matches = pd.merge(df_1, df_2, on=var, how='left',  suffixes=(self.frame__1.suffix, self.frame__2.suffix))
			matches = matches[~matches[self.frame__2.index].isnull()]

			matches = matches[[self.frame__1.index, self.frame__2.index]]
			if verbose:
				print(f'Address matches: {matches.shape[0]}')

			return matches

	def run(self, save=False, verbose=True):
		# Run individual matches according to attribute and concatenate.
		phone = self.phone(verbose=True)
		domain = self.domain(verbose=True)
		address = self.address(verbose=True)
		all_matches = pd.concat([phone, domain, address])

		# Create dummy column.
		# One important thing here is to group by first by self.frame__2.index since it has the One to Many relationship
		count_var = 'count__'
		all_matches[count_var] = 1
		all_matches = all_matches.groupby([self.frame__2.index, self.frame__1.index]).count().sort_values(count_var, ascending=False).reset_index()

		self.results = {}
		for c in all_matches[count_var].unique():
			temp = all_matches[all_matches[count_var] == c]
			r = pd.merge(self.frame__1.data, temp, on=self.frame__1.index)
			r = pd.merge(r, self.frame__2.data, on=self.frame__2.index, suffixes=(self.frame__1.suffix, self.frame__2.suffix))
			self.results[c] = r
			if save:
				r.to_csv(f'matches_{c}.csv', index=False)
			if verbose:
				print(f'Matches w/ {c} features: {r.shape[0]}')








