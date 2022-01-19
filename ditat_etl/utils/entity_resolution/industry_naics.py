import os

import spacy
import pandas as pd

from ...utils.time_functions import time_it


class NaicsStandard:
    @time_it()
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
        self.naics_df = pd.read_csv(os.path.join(self.filedir, 'naics.csv'), dtype={'Codes': str})
        self.naics_df.drop('Total Marketable US Businesses', axis=1, inplace=True)

        # Filtering level 3
        self.naics_df = self.naics_df[self.naics_df.Codes.str.len() <= 4]
        self.naics_df['level'] = self.naics_df.Codes.str.len() / 2
        self.naics_df['nlp'] = self.naics_df.Titles.apply(self.nlp)

    @time_it()
    def _classify(self, text):
        text_nlp = self.nlp(text)

        self.naics_df['similarity'] = self.naics_df.nlp.apply(lambda x: text_nlp.similarity(x))

        l2 = self.naics_df.loc[self.naics_df.level == 2, :].copy()

        l2.sort_values('similarity', ascending=False, inplace=True)

        top_2 = l2.iloc[0]

        naics_1_derived = top_2.Codes[0: 2] 

        title_1 = self.naics_df.loc[self.naics_df.Codes == naics_1_derived, 'Titles'].iloc[0]
        naics_1 = self.naics_df.loc[self.naics_df.Codes == naics_1_derived, 'Codes'].iloc[0]
        title_2 = top_2.Titles
        naics_2 = top_2.Codes

        return (title_1, naics_1, title_2, naics_2, top_2.similarity)

    @time_it()
    def classify(self, text: str or list):
        text = [text] if isinstance(text, str) else text

        unique_text = set(text)

        unique_results = {i: self._classify(i) for i in unique_text}

        results = [unique_results[i] for i in text]

        if len(results) == 1:
            return results[0]

        return results
