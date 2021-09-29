import json

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
        ):
        self.data = data.copy()
        self.name = name
        self.index = f"{index}_{name}"
        self.domain = f"{domain}_{name}"
        self.address = f"{address}_{name}"
        self.phone = f"{phone}_{name}"
        self.country = f"{country}_{name}"
        self.suffix = f"_{name}"

        self.data = self.data.add_suffix(f'_{name}')

class Matcher:
    def __init__(self):
        self._counter = 1
        self._names = []

    def set_df(
        self,
        data: pd.DataFrame,
        name: str,
        index='id',
        domain=None,
        address=None,
        phone=None,
        country=None
        ):
        '''
        Args:
            - data (pd.Dataframe)
            - name (str)
            - index (str, default='id')
            - domain (str, default=None)
            - address (str, default=None)
            - phone (str, default=None)
            - country (str, default=None)

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
            country=country
        )

        setattr(self, f"frame__{self._counter}", frame)
        
        self._counter += 1
        self._names.append(name)

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

            matches['match_type'] = 'phone'

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

            matches['match_type'] = 'domain'

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

            matches['match_type'] = 'address'

            return matches

    def run(self, save=False, verbose=True):
        # Run individual matches according to attribute and concatenate.
        phone = self.phone(verbose=True)
        domain = self.domain(verbose=True)
        address = self.address(verbose=True)
        all_matches = pd.concat([phone, domain, address])

        # Create dummy column.
        # One important thing here is to group by first by self.frame__2.index since it has the One to Many relationship
        all_matches = all_matches.groupby([self.frame__1.index, self.frame__2.index]).agg({'match_type': ['count', list]})
        columns = ['match_count', 'match_type']
        all_matches.columns = columns
        all_matches['match_type'] = all_matches['match_type'].apply(json.dumps)
        all_matches.sort_values(['match_count', 'match_type'], ascending=False, inplace=True)
        all_matches.reset_index(inplace=True)

        # Merge the original dataframes with the pivot 'all_matches'.
        results = pd.merge(self.frame__1.data, all_matches, on=self.frame__1.index)
        results = pd.merge(results, self.frame__2.data, on=self.frame__2.index, suffixes=(self.frame__1.suffix, self.frame__2.suffix))
        results.sort_values(['match_count', 'match_type'], ascending=False, inplace=True)

        for col in columns:
            f_col = results.pop(col)
            results.insert(0, col, f_col)

        self.results = results
        if save:
            self.results.to_csv(f'matches.csv', index=False)








