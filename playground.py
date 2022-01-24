'''
Test stuff
'''

from ditat_etl.utils.enrichment import PeopleDataLabs

pdl = PeopleDataLabs(
    api_key='cfc0a579cd65fb5d2783cf6fcfc648393bacbd8063213998810b9d25b1b6d7de',
    check_existing_method='s3',
    bucket_name='newfront-data-sets',
)

# r = pdl.search_company(
#         **{
#             'location.country': 'Chile',
#         },
#         return_size=1,
#         s3_recalculate=False
# )
# r = pdl.search_person(
#         **{
#             "job_company_website": "stripe.com"
#         },
#         return_size=1
# )
#
# print(r)
