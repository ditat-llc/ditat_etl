'''
Test stuff
'''
from ditat_etl.utils.enrichment import PeopleDataLabs

s3_config = {
    'bucket_name': 'newfront-data-sets',
    # 'account_enrichment_key': 'pdl-accounts-clean/',
    'account_search_key': 'pdl-accounts-clean/',
    # 'person_enrichment_key': None,
    # 'person_search_key': None,
    
}

p = PeopleDataLabs(
    api_key='cfc0a579cd65fb5d2783cf6fcfc648393bacbd8063213998810b9d25b1b6d7de',
    check_existing_method='s3',
    s3_config=s3_config
)

# Example: we only load account enrichment for now and
# we know that the name pathai already exists, we should catch
# the already existing record from s3


# r = p.enrich_company(name='pathai', save=False)
# r = p.enrich_company(profile='linkedin.com/company/pathai', save=False)
# r = p.enrich_company(website='acepta.com', save=True)


# r = p.search_company()
#
#
# print(r)
