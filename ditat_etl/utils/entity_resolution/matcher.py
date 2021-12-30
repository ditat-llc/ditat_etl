import json
import os
from functools import wraps

import pandas as pd

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
    
    @staticmethod
    def clean_field(x):
        x = x.lower()
        replacements = [",", ".", '"', "'", "!", "?", "/"]
        for r in replacements:
            x = x.replace(r, "")
        x = x.split()
        x = sorted(x)
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
        if self.exact_domain:
            df_1[var] = df_1[self.frame__1.domain]
            df_2[var] = df_2[self.frame__2.domain]
        else:
            df_1[var] = df_1[self.frame__1.domain].apply(extract_domain)
            df_2[var] = df_2[self.frame__2.domain].apply(extract_domain)

        df_1 = df_1[~df_1[var].isin(self.ignored_domains)]
        df_2 = df_2[~df_2[var].isin(self.ignored_domains)]

    @generic
    def phone(self, df_1, df_2, var):
        if self.frame__1.country is None or self.frame__2.country is None:
            return False

        df_1[var] = df_1.apply(lambda x: Phone.format(x[self.frame__1.phone], x[self.frame__1.country]), axis=1)
        df_2[var] = df_2.apply(lambda x: Phone.format(x[self.frame__2.phone], x[self.frame__2.country]), axis=1)

    def run(self, save=False, verbose=True):
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
        self.all_matches['match_type'] = self.all_matches['match_type'].apply(json.dumps)
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

        # Filtering according to match_type and match_count
        results = results.loc[ 
            (results.match_count >= 2)
            | (results.match_type == '["domain"]'),
            :
        ]
        print(f"Results filtered: {results.shape[0]}")

        self.results = results
        if save:
            self.results.to_csv(f'matches.csv', index=False)




    # def phone(self, verbose=False):
    #     if all([
    #         self.frame__1.phone,
    #         self.frame__1.country,
    #         self.frame__2.phone,
    #         self.frame__2.country
    #     ]):
    #         var = 'phone_fmt'
    #
    #         df_1 = self.frame__1.data.copy()
    #         df_2 = self.frame__2.data.copy()
    #
    #         df_1.dropna(subset=[self.frame__1.phone], inplace=True)
    #         df_2.dropna(subset=[self.frame__2.phone], inplace=True)
    #
    #         df_1[var] = df_1.apply(lambda x: Phone.format(x[self.frame__1.phone], x[self.frame__1.country]), axis=1)
    #         df_2[var] = df_2.apply(lambda x: Phone.format(x[self.frame__2.phone], x[self.frame__2.country]), axis=1)
    #
    #         df_1.dropna(subset=[var], inplace=True)
    #         df_2.dropna(subset=[var], inplace=True)
    #
    #         matches = pd.merge(df_1, df_2, on=var, how='left',  suffixes=(self.frame__1.suffix, self.frame__2.suffix))
    #         matches = matches[~matches[self.frame__2.index].isnull()]
    #
    #         matches = matches[[self.frame__1.index, self.frame__2.index, var]]
    #         if verbose:
    #             print(f'Phone matches: {matches.shape[0]}')
    #
    #         matches['match_type'] = 'phone'
    #
    #         return matches
    #
    # def domain(self, verbose=False):
    #     if all([
    #         self.frame__1.domain,
    #         self.frame__2.domain
    #     ]):
    #         var = 'domain_fmt'
    #
    #         df_1 = self.frame__1.data.copy()
    #         df_2 = self.frame__2.data.copy()
    #
    #         df_1.dropna(subset=[self.frame__1.domain], inplace=True)
    #         df_2.dropna(subset=[self.frame__2.domain], inplace=True)
    #
    #         if self.exact_domain:
    #             df_1[var] = df_1[self.frame__1.domain]
    #             df_2[var] = df_2[self.frame__2.domain]
    #         else:
    #             df_1[var] = df_1[self.frame__1.domain].apply(extract_domain)
    #             df_2[var] = df_2[self.frame__2.domain].apply(extract_domain)
    #
    #         # Skip domains that are part of self.ignored_domains
    #         # Evaluate moving this to the url.functions extract_domain
    #         df_1 = df_1[~df_1[var].isin(self.ignored_domains)]
    #         df_2 = df_2[~df_2[var].isin(self.ignored_domains)]
    #
    #         df_1.dropna(subset=[var], inplace=True)
    #         df_2.dropna(subset=[var], inplace=True)
    #
    #         matches = pd.merge(df_1, df_2, on=var, how='left',  suffixes=(self.frame__1.suffix, self.frame__2.suffix))
    #         matches = matches[~matches[self.frame__2.index].isnull()]
    #
    #         matches = matches[[self.frame__1.index, self.frame__2.index, var]]
    #         if verbose:
    #             print(f'Domain matches: {matches.shape[0]}')
    #
    #         matches['match_type'] = 'domain'
    #
    #         return matches
    #
    # def address(self, verbose=False):
    #     if all([
    #         self.frame__1.address,
    #         self.frame__2.address
    #     ]):
    #         var = 'address_fmt'
    #
    #         df_1 = self.frame__1.data.copy()
    #         df_2 = self.frame__2.data.copy()
    #
    #         df_1.dropna(subset=[self.frame__1.address], inplace=True)
    #         df_2.dropna(subset=[self.frame__2.address], inplace=True)
    #
    #         df_1[var] = df_1[self.frame__1.address]
    #         df_2[var] = df_2[self.frame__2.address]
    #
    #         # Custom
    #         df_1[var] = df_1[var].apply(type(self).clean_field)
    #         df_2[var] = df_2[var].apply(type(self).clean_field)
    #         ####
    #
    #         matches = pd.merge(df_1, df_2, on=var, how='left',  suffixes=(self.frame__1.suffix, self.frame__2.suffix))
    #         matches = matches[~matches[self.frame__2.index].isnull()]
    #
    #         matches = matches[[self.frame__1.index, self.frame__2.index, var]]
    #         if verbose:
    #             print(f'Address matches: {matches.shape[0]}')
    #
    #         matches['match_type'] = 'address'
    #
    #         return matches
    #
    # def entity_name(self, verbose=False):
    #     if all([
    #         self.frame__1.entity_name,
    #         self.frame__2.entity_name
    #     ]):
    #         var = 'entity_name_fmt'
    #
    #         df_1 = self.frame__1.data.copy()
    #         df_2 = self.frame__2.data.copy()
    #
    #         df_1.dropna(subset=[self.frame__1.entity_name], inplace=True)
    #         df_2.dropna(subset=[self.frame__2.entity_name], inplace=True)
    #
    #         df_1[var] = df_1[self.frame__1.entity_name]
    #         df_2[var] = df_2[self.frame__2.entity_name]
    #
    #         # Custom
    #         df_1[var] = df_1[var].apply(type(self).clean_field)
    #         df_2[var] = df_2[var].apply(type(self).clean_field)
    #         ####
    #
    #         matches = pd.merge(df_1, df_2, on=var, how='left',  suffixes=(self.frame__1.suffix, self.frame__2.suffix))
    #         matches = matches[~matches[self.frame__2.index].isnull()]
    #
    #         matches = matches[[self.frame__1.index, self.frame__2.index, var]]
    #         if verbose:
    #             print(f'Entity name matches: {matches.shape[0]}')
    #
    #         matches['match_type'] = 'entity_name'
    #
    #         return matches




