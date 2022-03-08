import os

import spacy
import pandas as pd

from ...time import TimeIt


class NaicsStandard:
    @TimeIt()
    def __init__(self, english_pipeline: str='en_core_web_lg'):
        self.filedir = os.path.abspath(os.path.dirname(__file__))

        self.load_nlp(english_pipeline=english_pipeline)
        self.load_naics()

    def load_nlp(self, english_pipeline):
        '''
        Create self.nlp

        It is necessary to have the vectors of words downloaded.
        This snippet downloads it automatically if not found (first time).
        '''
        try:
            self.nlp = spacy.load(english_pipeline)

        except Exception:
            os.system(f"python -m spacy download {english_pipeline}")
            self.nlp = spacy.load(english_pipeline)

    def load_naics(self):
        naics_full_path = os.path.join(self.filedir, 'naics.csv')
        self.naics_df = pd.read_csv(naics_full_path, dtype={'Codes': str})
        self.naics_df.drop('Total Marketable US Businesses', axis=1, inplace=True)

        # Filtering level 3
        self.naics_df = self.naics_df[self.naics_df.Codes.str.len() <= 4]
        self.naics_df['level'] = self.naics_df.Codes.str.len() / 2
        self.naics_df['nlp'] = self.naics_df.Titles.apply(self.nlp)

    def _classify(self, text, n=1, th=None):
        text_nlp = self.nlp(text)

        self.naics_df['similarity'] = self.naics_df.nlp.apply(
            lambda x: text_nlp.similarity(x)
        )

        l2 = self.naics_df.loc[self.naics_df.level == 2, :].copy()
        l2.drop(['nlp', 'level'], axis=1, inplace=True)

        l2['text'] = text

        l2.rename(columns={'Codes': 'naics_2', 'Titles': 'title_2'}, inplace=True)

        l2.sort_values('similarity', ascending=False, inplace=True)

        if th is not None:
            top_2 = l2[l2.similarity >= th].copy()

        else:
            top_2 = l2.iloc[0: n].copy()

        top_2['naics_1'] = top_2.naics_2.str[:2] 
        top_2['title_1'] = top_2.naics_1.map(
            self.naics_df[self.naics_df.level == 1].set_index('Codes')['Titles']
        )

        top_2 = top_2[[
            'text',
            'title_1',
            'naics_1',
            'title_2',
            'naics_2',
            'similarity'
        ]]

        print(f'Processed: {self.counter} / {self.total}', end='\r')
        self.counter += 1

        return top_2

    @TimeIt()
    def classify(self, text: str or list, n=1, th=None, as_df=True):
        text = [text] if isinstance(text, str) else text

        unique_text = set(text)

        self.total = len(unique_text)
        self.counter = 1 
        
        print(f'To be processed: {self.total}')

        results = [self._classify(i, n=n, th=th) for i in unique_text]

        df = pd.concat(results, axis=0, ignore_index=True)

        if as_df is False:
            df = df.to_dict(orient='records')

            if len(df) == 1:
                df = df[0]

        return df
