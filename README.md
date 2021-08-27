# Ditat ETL
Multiple tools and utilities for ETL pipelines and others.

## Utils
### _time_it_
Decorator to time function and class method. Additional text can be added.
```python
from ditat_etl.utils import time_it

@time_it()
def f():
  '''Do something'''
f()
```
```bash
f time: 0.1
```
## Url
Extension of module requests/urllib3 for Proxy usage and Bulk usage.

### _Url_
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
    conflict_on: list=None,
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
    verbose=False
):
```
