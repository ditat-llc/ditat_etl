import os

import pandas as pd

from ...time import TimeIt


'''
This class only includes logic for the United States of America.

Soon to support more.
'''

class StateStandard:
	def __init__(self) -> None:
		self.filedir = os.path.abspath(os.path.dirname(__file__))
		self.load_states()

	def load_states(self):
		filepath = os.path.join(self.filedir, 'us_states.csv')
		self.us_states = pd.read_csv(filepath)

	def _classify(self, text, verbose=True):
		if type(text) != str:
			return None

		text = str(text).lower().strip()

		## special case with hyphen
		if '-' in text:
			text = text.split('-')[1]
		###

		result = None

		for col in self.us_states:
			if text in self.us_states[col].values:

				result = self.us_states.loc[
					self.us_states[col] == text, 'state'
				].iloc[0]

				break

		if verbose:
			print(f'Processed: {self.counter} / {self.total}', end='\r')
		self.counter += 1

		return result

	@TimeIt()
	def classify(self, text, verbose=True):
		text = [text] if isinstance(text, str) else text

		unique_text = set(text)

		self.total = len(unique_text)
		self.counter = 1 
		
		print(f'To be processed: {self.total}')


		results = {i: self._classify(i, verbose) for i in unique_text}

		return results
