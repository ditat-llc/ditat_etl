import os
import json
from io import BytesIO
from concurrent.futures import ThreadPoolExecutor

import requests
import pandas as pd
import boto3

from ... import time_it
from ....url.functions import extract_domain
from ....databases import Postgres


class PeopleDataLabs:
    VERSION = 'v5'

    BASE_URL = f'https://api.peopledatalabs.com/{VERSION}'

    SAVE_DIRS = [  
        'account_enrichment',
        'account_search',
        'person_enrichment',
        'person_search',
    ]

    def __init__(
        self,
        api_key: str,
        check_existing_method: str='s3',
        db_config=None,
        **kwargs

    ) -> None:
        '''
        Args:
            - api_key (str): People Data Labs api_key
            - check_existing_method (str): {local, s3, (others coming soon)}
        '''
        self.api_key = api_key
        self.check_existing_method = check_existing_method
        self.db_config = db_config
        self.init(**kwargs)

    def init(self, **kwargs):
        # Check existing files locally
        if self.check_existing_method == 'local': 
            for dir in type(self).SAVE_DIRS:
                if not os.path.exists(dir):
                    os.makedirs(dir)

        elif self.check_existing_method == 's3':
            self.s3_params = kwargs
            self.s3_setup(**kwargs)

    @time_it()
    def s3_setup(
        self,
        bucket_name,
        account_enrichment_key: str=None,
        account_search_key: str=None,
        person_enrichment_key: str=None,
        person_search_key: str=None,
        **kwargs
    ):
        s3_payload = {'service_name': 's3'}

        if 'aws_access_key_id' in kwargs and 'aws_secret_access_key' in kwargs:
            s3_payload['aws_access_key_id'] = kwargs['aws_access_key_id'] 
            s3_payload['aws_secret_access_key'] = kwargs['aws_secret_access_key'] 

        self.s3_resource = boto3.resource(**s3_payload)
        self.s3_client = boto3.client(**s3_payload)

        self.bucket_name = bucket_name
        self.bucket = self.s3_resource.Bucket(self.bucket_name)

        self.s3_folders = {
            's3_ae': account_enrichment_key or 'account_enrichment',
            's3_as': account_search_key or 'account_search',
            's3_pe': person_enrichment_key or 'person_enrichment',
            's3_ps': person_search_key or 'person_search',
        }
        self.s3_folders = {i: j for i, j in self.s3_folders.items() if j}

        # @time_it()
        def _read_file_from_s3(file):
            # print(f'Processing: {file.key}')
            try:
                fmt_file = file.get()['Body'].read().decode('UTF-8')
                df = pd.json_normalize(json.loads(fmt_file))
                # print(f'Finishing: {file.key}')
                return df
            except Exception:
                pass
                # print(f"Error: {file.key}")
        
        for key, value in self.s3_folders.items():
            print(f"Starting: {value} setup")

            filtered_files = self.bucket.objects.filter(Prefix=f"{value}/").all()
            filtered_files = [f for f in filtered_files if f.key != f"{value}/"]

            with ThreadPoolExecutor(max_workers=min(1000, len(filtered_files))) as ex:
                results = ex.map(_read_file_from_s3, filtered_files)
            dfs = [df for df in results if df is not None]

            if dfs:
                joined_df = pd.concat(dfs, axis=0, ignore_index=True)
                setattr(self, key, joined_df)

        print('Finished: s3_setup')

    def to_db(self, tablename:str, db_config: dict=None, conflict_on: str='domain'):
        '''
        For now the 2 dataframes that are pushed to the DB are:
            i. self.s3_ae
            ii. self.s3_as 
        '''
        column_mapping = {
            'name': 'name',
            'employee_count': 'employee_count',
            'founded': 'founded',
            'industry': 'industry',
            'linkedin_url': 'linkedin_url',
            'facebook_url': 'facebook_url',
            'twitter_url': 'twitter_url',
            'website': 'website',
            'ticker': 'ticker',
            'type': 'type',
            'tags': 'tags',
            'alternative_names': 'alternative_names',
            'location.locality': 'city',
            'location.region': 'state',
            'location.country': 'country',
            'location.street_address': 'street',
            'location.postal_code': 'zip',
        }
        self.db = Postgres(db_config)
        
        # Account Enrichment Processing 
        if hasattr(self, 's3_ae'):
            print(f'Pushing self.s3_ae to table: {tablename}')
            s3_ae_db = self.s3_ae.copy()
            s3_ae_db.rename(columns=column_mapping, inplace=True)
            s3_ae_db = s3_ae_db[column_mapping.values()]

            s3_ae_db['domain'] = s3_ae_db['website'].apply(extract_domain)
            s3_ae_db = s3_ae_db.loc[(s3_ae_db['domain'] != 'nan') | (~s3_ae_db['domain'].isnull()), :]

            s3_ae_db['data_source'] = 'people_data_labs'

            self.db.insert_df_to_sql(
                df=s3_ae_db,
                tablename=tablename,
                commit=True,
                conflict_on=conflict_on,
                do_update_columns=False,
                verbose=False
            )
        # Account Search Processing
        if hasattr(self, 's3_as'):
            print(f'Pushing self.s3_as to table: {tablename}')
            s3_as_db = self.s3_as.copy()
            s3_as_db.rename(columns=column_mapping, inplace=True)
            s3_as_db = s3_as_db[column_mapping.values()]

            s3_as_db['domain'] = s3_as_db['website'].apply(extract_domain)
            s3_as_db = s3_as_db.loc[(s3_as_db['domain'] != 'nan') | (~s3_as_db['domain'].isnull()), :]

            s3_as_db['data_source'] = 'people_data_labs'

            self.db.insert_df_to_sql(
                df=s3_as_db,
                tablename=tablename,
                commit=True,
                conflict_on=conflict_on,
                do_update_columns=False,
                verbose=False
            )

    def aggregate(self, dir_type: str):
        if dir_type not in type(self).SAVE_DIRS:
            raise ValueError('Not a valid dir_type. Check PeopleDataLabs.SAVE_DIRS')

        files = os.listdir(dir_type)

        dfs = []
       
        for file in files:
            path = os.path.join(dir_type, file)
            with open(path, 'r') as f:
                file = json.loads(f.read())

            df = pd.json_normalize(file)
            dfs.append(df)

        if not dfs:
            return None
        agg = pd.concat(dfs, axis=0, ignore_index=True)
        return agg

    def enrich_company(
        self,
        min_likelihood: int=2,
        required=None,
        save=True,
        check_existing=True,
        s3_recalculate=True,
        **kwargs
    ):
        # Checking minimum fields.
        required_fields = {
            'name': ['name'],
            'profile': ['linkedin_url', 'facebook_url', 'twitter_url'],
            'ticker': ['ticker'],
            'website': ['website']
        }

        if not any(i in required_fields for i in kwargs):
            raise ValueError(f'You need to specify at least one of {required_fields}')

        if 'website' in kwargs:
            kwargs['website'] = extract_domain(kwargs['website'])
            if kwargs['website'] is None:
                print('Not a valid domain.')
                return None

        # Process to check if file company has already been enriched.
        if check_existing and self.check_existing_method == 'local':
            existing_files = []
            existing_filenames =[f"account_enrichment/{i}" for i in os.listdir('account_enrichment')] 
            for file in existing_filenames:
                with open(file, 'r') as f:
                    file_data = json.loads(f.read())
                    existing_files.append(file_data)

            for existing_file in existing_files:
                for required_field in required_fields:
                    if required_field in kwargs:

                        if kwargs[required_field] in existing_file[required_field] or \
                        existing_file[required_field] in kwargs[required_field]:
                            print(f"{required_field}: {kwargs[required_field]} already exists in local.")
                            return None
        
        # Using self.s3_ae to check existing
        elif check_existing and self.check_existing_method == 's3':
            if hasattr(self, 's3_ae'):
                for key, value in required_fields.items():
                    if key in kwargs:
                        for v in value:
                            if kwargs[key] in self.s3_ae[v].values:
                                print(f"{key}: {kwargs[key]} already exists in s3.")
                                return None
        #####
        url = f"{type(self).BASE_URL}/company/enrich"

        params = {
            "api_key": self.api_key,
            "min_likelihood": min_likelihood,
            "required": required
        }
        params.update(kwargs)

        json_response = requests.get(url, params=params).json()

        if json_response["status"] == 200:
            if save and self.check_existing_method == 'local':
                with open(os.path.join('account_enrichment', f"{json_response['id']}.json"), 'w') as out:
                    out.write(json.dumps(json_response))

            elif save and self.check_existing_method == 's3':
                fmt_filename = f"{self.s3_folders['s3_ae']}/{json_response['id']}.json"
                fmt_file = BytesIO(json.dumps(json_response).encode('UTF-8'))

                self.s3_client.upload_fileobj(fmt_file, self.bucket_name, fmt_filename)        
                
                if s3_recalculate:
                    self.s3_setup(**self.s3_params)

        return json_response

    def search_company(
        self,
        required: str=None,
        strategy='AND',
        size=1,
        check_existing=True,
        save=True,
        verbose=True,
        s3_recalculate=True,
        **kwargs,
    ):
        url = f"{type(self).BASE_URL}/company/search"

        H = {
          'Content-Type': "application/json",
          'X-api-key': self.api_key
        }

        # SQL query construction
        sql = f"SELECT * FROM company WHERE"

        if kwargs:
            where_str = f' {strategy} '.join([f"{k} = '{v}'" for k, v in kwargs.items()])
            sql += ' '
            sql += where_str

        if required:
            if kwargs:
                sql += ' AND'
            sql += f' {required} IS NOT NULL'

        if check_existing and self.check_existing_method == 'local':
            existing = self.aggregate(dir_type='account_search')
            if existing is not None:
                existing = existing.website.tolist()
                existing_str =  ' AND website NOT IN (' + ', '.join([f"'{website}'" for website in existing]) + ')'
                sql += existing_str

        elif check_existing and self.check_existing_method == 's3':
            if hasattr(self, 's3_as'):
                existing = self.s3_as.website.unique().tolist()
                existing_str =  ' AND website NOT IN (' + ', '.join([f"'{website}'" for website in existing]) + ')'
                sql += existing_str

        if verbose:
            print(sql)

        P = {
          'sql': sql,
          'size': size,
          'pretty': True
        }

        response = requests.get(
          url,
          headers=H,
          params=P
        ).json()

        if verbose:
            print(response)

        if response['status'] == 200:
            for company in response['data']:
                id = company['id']

                if save and self.check_existing_method == 'local':
                    with open(f'account_search/{id}.json', 'w') as out:
                        out.write(json.dumps(company))

                elif save and self.check_existing_method == 's3':
                    fmt_filename = f"{self.s3_folders['s3_as']}/{company['id']}.json"
                    fmt_file = BytesIO(json.dumps(company).encode('UTF-8'))
                    self.s3_client.upload_fileobj(fmt_file, self.bucket_name, fmt_filename)        

                    if s3_recalculate:
                        self.s3_setup(**self.s3_params)

            return response['data']
    
    def search_person(
        self,
        required: str='work_email',
        strategy="AND",
        size=1,
        check_existing=True,
        save=True,
        verbose=True,
        s3_recalculate=True,
        **kwargs
    ):

        url = f"{type(self).BASE_URL}/person/search"

        H = {
          'Content-Type': "application/json",
          'X-api-key': self.api_key
        }

        # SQL query construction
        sql = f"SELECT * FROM person WHERE"

        if kwargs:
            where_str = f' {strategy} '.join([f"{k} LIKE '%{v}%'" for k, v in kwargs.items()])
            sql += ' '
            sql += where_str

        if required:
            if kwargs:
                sql += ' AND'
            sql += f' {required} IS NOT NULL'

        if check_existing and self.check_existing_method == 'local':
            existing = self.aggregate(dir_type='person_search')
            if existing is not None:
                existing = existing['work_email'].tolist()
                existing_str =  ' AND work_email NOT IN (' + ', '.join([f"'{i}'" for i in existing]) + ')'
                sql += existing_str

        elif check_existing and self.check_existing_method == 's3':
            if hasattr(self, 's3_ps'):
                existing = self.s3_ps['work_email'].tolist()
                existing_str =  ' AND work_email NOT IN (' + ', '.join([f"'{i}'" for i in existing]) + ')'
                sql += existing_str

        if verbose:
            print(sql)

        P = {
          'sql': sql,
          'size': size,
          'pretty': True
        }

        response = requests.get(
          url,
          headers=H,
          params=P
        ).json()

        if response['status'] == 200:
            for person in response['data']:
                id = person['id']

                if save and self.check_existing_method == 'local':
                    with open(f'person_search/{id}.json', 'w') as out:
                        out.write(json.dumps(person))

                elif save and self.check_existing_method == 's3':
                    fmt_filename = f"{self.s3_folders['s3_ps']}/{id}.json"
                    fmt_file = BytesIO(json.dumps(person).encode('UTF-8'))
                    self.s3_client.upload_fileobj(fmt_file, self.bucket_name, fmt_filename)        

                    if s3_recalculate:
                        self.s3_setup(**self.s3_params)

        return response

    def enrich_person(
        self,     
        min_likelihood: int=2,
        required=None,
        save=True,
        check_existing=True,
        s3_recalculate=True,
        verbose=True,
        **kwargs
    ):
        # Checking minimum fields.
        required_fields = {
            'profile': ['linkedin_url', 'facebook_url', 'twitter_url'],
            'email': ['email', 'personal_emails', 'emails'],
            'phone': ['phone']
        }

        if not any(i in required_fields for i in kwargs):
            if all(i in ['first_name', 'last_name', 'company'] for i in kwargs):
                pass
            else:
                raise ValueError(f'You need to specify at least one of {required_fields}')

        # Process to check if file company has already been enriched.
        if check_existing and self.check_existing_method == 'local':
            existing_files = []
            existing_filenames =[f"person_enrichment/{i}" for i in os.listdir('person_enrichment')] 
            for file in existing_filenames:
                with open(file, 'r') as f:
                    file_data = json.loads(f.read())
                    existing_files.append(file_data)

            for existing_file in existing_files:
                if 'email' in kwargs:
                    if required == existing_file['work_email'] or required in existing_file['personal_emails']: # Add emails
                        print(f"Email already exists.")
                        return None

                elif 'profile' in kwargs:
                    for profile in ['facebook_url', 'linkedin_url', 'twitter_url', 'github_url']:
                        if required == existing_file[profile]:
                            print(f"Profile already exists.")
                            return None

                elif 'phone' in kwargs:
                    if required == existing_file['mobile_phone'] or required in existing_file['phone_numbers']:
                        print(f"Phone already exists.")
                        return None
                else:
                    pass
                    # Pending for combo first_name, last_name and company


        elif check_existing and self.check_existing_method == 's3':
            if hasattr(self, 's3_pe'):
                if 'email' in kwargs:
                    d = kwargs['email']
                    if d in self.s3_pe['work_email'].values \
                    or self.s3_pe['personal_emails'].astype(str).str.contains(d).any() \
                    or self.s3_pe['emails'].astype(str).str.contains(d).any():
                        print(f"Email already exists.")
                        return None

                elif 'profile' in kwargs:
                    d = kwargs['profile']
                    for profile in ['facebook_url', 'linkedin_url', 'twitter_url', 'github_url']:
                        if d in self.s3_pe[profile].values:
                            print(f"Profile already exists.")
                            return None

                elif 'phone' in kwargs:
                    d = kwargs['phone']
                    if d in self.s3_pe['mobile_phone'].values or self.s3_pe['phone_numbers'].astype(str).str.contains(d).any():
                        print(f"Phone already exists.")
                        return None

                elif all(i in ['first_name', 'last_name', 'company'] for i in kwargs):
                    filtered_df = self.s3_pe.loc[ 
                        (self.s3_pe['first_name'] == kwargs['first_name'].lower())
                        & (self.s3_pe['last_name'] == kwargs['last_name'].lower())
                        & (self.s3_pe['job_company_name'] == kwargs['company'].lower())
                    ]
                    if filtered_df.shape[0] > 0:
                        print('first_name, last_name, company already exists.')
                        return None

        url = f"{type(self).BASE_URL}/person/enrich"

        params = {
            "api_key": self.api_key,
            "min_likelihood": min_likelihood,
            "required": required
        }
        params.update(kwargs)

        json_response = requests.get(url, params=params).json()

        if verbose:
            print(json_response['status'])

        if json_response["status"] == 200:
            data = json_response['data']
            filename = f"{json_response['data']['id']}.json"

            if save and self.check_existing_method == 'local':
                with open(os.path.join('person_enrichment', f"{filename}"), 'w') as out:
                    out.write(json.dumps(data))

            elif save and self.check_existing_method == 's3':
                fmt_filename = f"{self.s3_folders['s3_pe']}/{filename}"
                fmt_file = BytesIO(json.dumps(data).encode('UTF-8'))

                self.s3_client.upload_fileobj(fmt_file, self.bucket_name, fmt_filename)        

                if s3_recalculate:
                    self.s3_setup(**self.s3_params)

            return data
