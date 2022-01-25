'''
Test stuff
'''

from ditat_etl.utils.enrichment import PeopleDataLabs

pdl = PeopleDataLabs(
    api_key='xxx',
    check_existing_method='s3',
    bucket_name='newfront-data-sets',
)

r = pdl.search_person(
        job_title_role=['finance', 'legal', 'real_estate', 'operations'],
        job_title_levels=['cxo','director','owner','vp'],
        job_company_website=['botc.com', 'aiains.com', 'startupweekend.org'],
        return_size=1
)

