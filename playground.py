'''
Test stuff
'''


from ditat_etl.utils.enrichment import PeopleDataLabs

pdl = PeopleDataLabs(
	api_key='cfc0a579cd65fb5d2783cf6fcfc648393bacbd8063213998810b9d25b1b6d7de',
	check_existing_method='s3',
	aws_access_key_id = 'AKIAVKQ5KEXP6GQDYDR2',
	aws_secret_access_key = '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',
	bucket_name = 'revtron-peopledatalabs',
)

# p = pdl.enrich_person(
# 	min_likelihood=5,
# 	required=None,
# 	save=True,
# 	check_existing=True,
# 	s3_recalculate=True,
# 	verbose=True,
# 	# name='Dylan Dempsey',
# 	email='dylan@ditat.io'
#
#
# )
# print(p)

# p = pdl.enrich_company(
# 	min_likelihood=5,
# 	required=None,
# 	save=True,
# 	check_existing=True,
# 	s3_recalculate=True,
# 	name='acepta.com'
#
#
# )
print(pdl.s3_ae.T)

print(pdl.s3_pe.T.head(60))

