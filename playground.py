'''
Test stuff
'''


from ditat_etl.utils.enrichment import PeopleDataLabs
import pandas as pd
import numpy as np

pdl = PeopleDataLabs(
	api_key='xxx',
	check_existing_method='s3',
	aws_access_key_id = 'xxx',
	aws_secret_access_key = 'xxx',
	bucket_name = 'revtron-peopledatalabs',
)

# df = pd.read_csv('gigpro-acts-no-naics.csv')
# df.dropna(subset=['website'], inplace=True)
# df = df.iloc[0: 10]
# print(df)
#
# df.rename(columns={
# 	'tickersymbol': 'ticker',
# 	'billingcountry': 'country',
# 	'billingstreet': 'location',
# 	}, inplace=True
# )
# columns = ['name', 'website', 'phone', 'country', 'location', 'ticker']
# df = df[columns]
# print(df)
#
# data_list = df.to_dict(orient='records')
#
# r = pdl.bulk_enrich_account(
# 	account_list=data_list,
# 	min_likelihood=5,
# 	required='naics'
# )
# print(r)
#
# # p = pdl.enrich_person(
# # 	min_likelihood=5,
# # 	required=None,
# # 	save=True,
# # 	check_existing=True,
# # 	s3_recalculate=True,
# # 	verbose=True,
# # 	# name='Dylan Dempsey',
# # 	email='dylan@ditat.io'
# #
# #
# # )
# # print(p)
#
p = pdl.enrich_account(
	min_likelihood=5,
	required=None,
	save=True,
	check_existing=True,
	s3_recalculate=True,
	name='fasdasd',
	index='asuidnasiduasn'


)
print(p)
# # print(pdl.s3_ae.T)
# #
# # print(pdl.s3_pe.T.head(60))
#
