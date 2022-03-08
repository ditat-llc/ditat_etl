import os

import pandas as pd
import phonenumbers


filedir = os.path.abspath(os.path.dirname(__file__))


class Phone:
	CODES = pd.read_csv(os.path.join(filedir, 'country_codes.csv'))

	def __init__(self):
		pass

	@classmethod
	def format(cls, phone, country):
	    country = str(country).lower()
	    if country in cls.CODES.iso_long.values:
	        country = cls.CODES.loc[cls.CODES.iso_long == country, 'iso'].values[0]
	    elif country in cls.CODES.country.values:
	        country = cls.CODES.loc[cls.CODES.country == country, 'iso'].values[0]
	    try:
	        return phonenumbers.format_number(phonenumbers.parse(phone, country.upper()), phonenumbers.PhoneNumberFormat.E164)
	    except Exception:
	        return None
