import os
import time

import spacy
import pandas as pd

from ...time import TimeIt


class CountryStandard:
    # @TimeIt()
    def __init__(self, english_pipeline: str='en_core_web_lg', use_nlp=False):
        self.filedir = os.path.abspath(os.path.dirname(__file__))

        self.use_nlp = use_nlp
        self.load_nlp(english_pipeline=english_pipeline)

        self.load_countries()

    def load_nlp(self, english_pipeline):
        '''
        Create self.nlp

        It is necessary to have the vectors of words downloaded.
        This snippet downloads it automatically if not found (first time).
        '''
        if self.use_nlp:
            try:
                self.nlp = spacy.load(english_pipeline)

            except Exception:
                os.system(f"python -m spacy download {english_pipeline}")
                self.nlp = spacy.load(english_pipeline)

    def load_countries(self):
        '''
        This assumes a fixed directory for the csv file.

        '''
        filepath = os.path.join(self.filedir, '../country_codes.csv')
        self.country_codes = pd.read_csv(filepath)

    def _classify(self, text):
        if type(text) != str:
            return None

        text = str(text).lower().strip()

        ## special cases
        if text in ['uk', 'england']:
            text = 'united kingdom'
        ##

        found = False
        result = None

        for col in self.country_codes:
            if text in self.country_codes[col].values:
                
                found = True

                result = self.country_codes.loc[
                    self.country_codes[col] == text, 'country'
                ].iloc[0]

                break

        if found is False and self.use_nlp:

            text_nlp = self.nlp(text)

            c = self.country_codes.copy()

            c['similarity'] = c.country.apply(lambda x: text_nlp.similarity(self.nlp(x)))

            c.sort_values('similarity', ascending=False, inplace=True)

            similarity = c.iloc[0].similarity
            country = c.iloc[0].country

            if similarity > 0.80:
                print(text, country)
                result = country

        print(f'Processed: {self.counter} / {self.total}', end='\r')
        self.counter += 1

        return result

    @TimeIt()
    def classify(self, text):
        text = [text] if isinstance(text, str) else text

        unique_text = set(text)

        self.total = len(unique_text)
        self.counter = 1 
        
        print(f'To be processed: {self.total}')


        results = {i: self._classify(i) for i in unique_text}

        return results





