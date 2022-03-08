# Ditat ETL
Multiple tools and utilities for ETL pipelines and others.

## Databases
Useful wrappers for databases and methods to execute queries.

### _Postgres_
It is compatible with pandas.DataFrame interaction, either reading as dataframes and pushing to the db.
```python
from ditat_etl.databases import Postgres

config = {
    "database": "xxxx",
    "user": "xxxx",
    "password": "xxxx",
    "host": "xxxxx",
    "port": "xxxx"
}
p = Postgres(config)
```
The main base function is query.
```python
p.query(
  query_statement: list or str,
  df: bool=False,
  as_dict: bool=False,
  commit: bool=True,
  returning: bool=True,
  mogrify: bool=False,
  mogrify_tuple: tuple or list=None,
  verbose=False
)
```
This function is a workaround of pandas.to_sql() which drops the table before inserting.
It really works like an upsert and it gives you the option to do nothing or update on the column(s) constraint.
```python
p.insert_df_to_sql(
  df: pd.DataFrame,
  tablename: str,
  commit=True,
  conflict_on: str or list=None,
  do_update_columns: bool or list=False,
  verbose=False
):
```
This one is similar, it lets you "upsert" without necessarily having a primary key or constraint.
Ideally use the previous method.
```python
p.update_df_to_sql(
  df: pd.DataFrame,
  tablename: str,
  on_columns: str or list,
  insert_new=True,
  commit=True,
  verbose=False,
  overwrite=False
):
```
This class also has some smaller utitlies for describing tables, getting information,
creating tables and indexes among others.


## Salesforce

### _SalesforceObj_
Salesforce wrapper adding functionalities to Simple Salesforce.

You can connect with login crendentials or through the Oauth2 protocol with 
a refresh token and client crendentials.
```python
from ditat_etl.salesforce import SalesforceObj

sf_config = {
  "organizationId": "xxx",
  "password": "xxx",
  "security_token": "xxx",
  "username": "xxx",
  # Session Id for refresh token
  "session_id": "xxx"
}

# With login credentials
sf = Salesforce(sf_config)

# With Oauth2 refresh token and client credentials
sf = SalesforceObj(
  sf_config,
  client_id='xxx',
  client_secret='xxx',
)
```
The core of this class is the query method.
```python
sf.query(
  tablename='xxx',
  columns=None,
  limit=None,
  df=False,
  date_from=None,
  date_to=None,
  date_window=None,
  date_window_variable='LastModifiedDate',
  verbose=False,
  timeout=None,
  max_columns=100,
)

```
Most times you can directly use the parallel execution.
```python
sf.query_parallel(
  tablename='xxx',
  columns=None,
  limit=None,
  df=False,
  date_window=None,
  date_window_variable='LastModifiedDate',
  verbose=False,
  n_chunks=4
)
```

Update records
```python
sf.update(
  tablename="xxx",
  record_list=[{'Id': '11111', 'field1': '1111'}],
  batch_size=1000
)
```
Insert records (update as an option)
```python
sf.insert_df(
  tablename='xxx',
  dataframe=pd.DataFrame,
  batch_size=10000,
  update=True,
  conflict_on='Name',
  return_response=False,
)
```

## Entity Resolution and Deduplication
The Matcher allows you to match two datasets on certain criteria
and also find duplicates.

### _Matcher_
```python
from ditat_etl.utils.entity_resolution import Matcher

matcher = Matcher(exact_domain=False)
```
### *Entity Resolution*
Set both datasets as dataframes. Specify the names of the columns to use
as criteria for matching
```python
# Set df 1
matcher.set_df(
  data=pd.DataFrame,
  name='df_1',
  index='id',
  domain=None,
  address=None,
  phone=None,
  country=None,
  entity_name=None
)
# Set df 2 
matcher.set_df(
  data=pd.DataFrame,
  name='df_2',
  index='id',
  domain=None,
  address=None,
  phone=None,
  country=None,
  entity_name=None
)

# Run
matches = matches.run(
  save=False,
  verbose=True,
  deduping=False,
  match_type_included=None,
  match_type_excluded=None,
  match_count_th=3
)
```
### *Deduplication*
```python
results = matcher.dedupe(
  data=pd.DataFrame,
  index='id',
  domain=None,
  address=None,
  phone=None,
  country=None,
  entity_name=None,
  save=True,
  match_type_included=None,
  match_type_excluded=None,
  match_count_th=3,
  include_self=True
)
```
## TimeIt
The TimeIt class helps you time functions, methods, blocks of code.
```python
from ditat_etl.time import TimeIt

A)
	@TimeIt
	def f():
		pass

	f()

	>> f takes: 0.0006 sec.

B) 
	@TimeIt(decimals=8)
	def f():
		pass

	f()

	>> f takes: 0.00000003 sec.

C) 
	with TimeIt():
		time.sleep(0.3)

	>> indented block takes: 0.3005 sec.

D) 
	class Foo:
	@TimeIt() # It has to be a callable for classes
	def m(self):
		pass

	Foo().m()

	>> m takes: 0.0 sec.
```

## Enrichment
### _PeopleDataLabs_

```python
from ditat_etl.utils.enrichment import PeopleDataLabs

pdl = PeopleDataLabs(
    api_key='xxx',
    check_existing_method='s3',
    bucket_name='xxx',
)
```
You can:
- Enrich company
- Search company
- Enrich person
- Search person

```python
r = pdl.enrich_company(
	min_likelihood=5,
	required='website',
	save=True,
	check_existing=True,
	s3_recalculate=False,
	company_name='companyx'
)

'''
PLEASE CHECK https://docs.peopledatalabs.com/docs 
FOR MORE DETAILS

'''
```

## Url

### _functions_
```python
from ditat_etl.url.functions import eval_url, extract_domain

data = [
    'laskdj',
    'www.google.com',
    'accounts.google.com',
    'https://www.emol.com/economia/'
]

for d in data:
    r = extract_domain(d)
    print(r)

print('####')
r = eval_url(data)

print(r)
```
```bash
None
google.com
accounts.google.com
emol.com
####
Initializing 4 workers.
eval_url takes: 1.3309 sec.
{'laskdj': None, 'www.google.com': 'google.com', 'accounts.google.com': 'accounts.google.com', 'https://www.emol.com/economia/': 'emol.com'}
```

### _Url_
Extension of module requests/urllib3 for Proxy usage and Bulk usage.
*High-level usage*
```python
from ditat_etl import url

response = url.get('https://google.com')
# You can pass the same parameters as the library requests and other special parameters.

# Check low level usage for more details.
```
*Low-level usage*
```python
from ditat_etl.url import Url

u = Url()
```
We use the logging module and it is set by default with 'DEBUG'.
You can change this parameter to any allowed level
```python3
u = Url(debug_level='WARNING') # Just an example
```
Manage your proxies
```python
u.add_proxies(n=3) # Added 3 new proxies (not necessarily valid) to self.proxies

u.clean_proxies() # Multithreaded to validate and keep only valid proxies.
```
```python
print(u.proxies)
# You can also u.proxies = [], set them manually but this is not recommended.
```

#### Main functionality
```python
def request(
    queue: str or list,
    expected_status_code: int=200,
    n_times: int=1,
    max_retries: int=None,
    use_proxy=False,
    _raise=True,
    ***kwargs
    ):
```
Examples
```python

result = u.request('https://google.com')

result = u.request(queue=['https://google.com', 'htttps://facebook.com'], use_proxy=True)

# You can also pass optional parameter valid por a requests "Request"
import json
result = u.request(queue='https://example.com', method='post', data=json.dumps({'hello': 'world'}))
```
