'''
Test stuff
'''

from ditat_etl.utils.enrichment import PeopleDataLabs

pdl = PeopleDataLabs(
    api_key='xxxxxxxxxxxxxxxxxxxxxxxxxxxxxx',
    check_existing_method='s3',
    bucket_name='newfront-data-sets',
)

r = pdl.search_company(
        **{
            'location.country': 'Chile',
        },
        return_size=1,
        s3_recalculate=False
)

print(r)
