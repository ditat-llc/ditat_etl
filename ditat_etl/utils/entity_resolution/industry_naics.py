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

        self.l1 = self.naics_df[self.naics_df.level == 1]
        self.l2 = self.naics_df[self.naics_df.level == 2]

        # self.naics_1 = {
        #     self.nlp(i): j for i, j in \
        #     self.naics_df[self.naics_df.Codes.str.len() == 2].set_index('Titles')['Codes'].to_dict().items()
        # }
        # self.naics_2 = {
        #     self.nlp(i): j for i, j in \
        #     self.naics_df[self.naics_df.Codes.str.len() == 4].set_index('Titles')['Codes'].to_dict().items()
        # }
        # self.naics_3 = {
        #     self.nlp(i): j for i, j in \
        #     self.naics_df[self.naics_df.Codes.str.len() == 6].set_index('Titles')['Codes'].to_dict().items()
        # }

    @time_it()
    def classify(self, text, th_1=0.9, th_2=0.7):
        text_nlp = self.nlp(text)

        l1 = self.l1.copy()
        l2 = self.l2.copy()

        l1['similarity'] = l1.nlp.apply(lambda x: text_nlp.similarity(x))
        l2['similarity'] = l2.nlp.apply(lambda x: text_nlp.similarity(x))

        l1.sort_values('similarity', ascending=False, inplace=True)
        l2.sort_values('similarity', ascending=False, inplace=True)

        top_1 = l1.iloc[0]
        top_2 = l2.iloc[0]

        naics_1_derived = top_2.Codes[0: 2] 

        title_1 = l1.loc[l1.Codes == naics_1_derived, 'Titles'].iloc[0]
        naics_1 = l1.loc[l1.Codes == naics_1_derived, 'Codes'].iloc[0]
        title_2 = top_2.Titles
        naics_2 = top_2.Codes

        return (title_1, naics_1, title_2, naics_2, top_2.similarity)

        # score analysis
        # if top_2.similarity > th_1:
        #     title_1 = l1.loc[l1.Codes == naics_1_derived, 'Titles'].iloc[0]
        #     naics_1 = l1.loc[l1.Codes == naics_1_derived, 'Codes'].iloc[0]
        #     title_2 = top_2.Titles
        #     naics_2 = top_2.Codes
        #     return (title_1, naics_1, title_2, naics_2)

        # if top_1.similarity > th_2 and top_2.similarity > th_2 and naics_1_derived == top_1.Codes:
        #     title_1 = top_1.Titles
        #     naics_1 = top_1.Codes
        #     title_2 = top_2.Titles
        #     naics_2 = top_2.Codes
        #     return (title_1, naics_1, title_2, naics_2)
        #
        # 
        # elif top_2.similarity > 0.70 and naics_1_derived == top_1.Codes:
        #     title_1 = l1.loc[l1.Codes == naics_1_derived, 'Titles'].iloc[0]
        #     naics_1 = l1.loc[l1.Codes == naics_1_derived, 'Codes'].iloc[0]
        #     title_2 = top_2.Titles
        #     naics_2 = top_2.Codes
        #
        #     return (title_1, naics_1, title_2, naics_2)

        # return (None, None, None, None, None)

    @time_it()
    def classify_bulk(self, text: str or list):
        text = [text] if isinstance(text, str) else text

        unique_text = set(text)

        unique_results = {i: self.classify(i) for i in unique_text}

        results = [unique_results[i] for i in text]

        if len(results) == 1:
            return results[0]

        return results
