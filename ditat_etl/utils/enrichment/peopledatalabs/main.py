import os
import json

import requests
import pandas as pd


class PeopleDataLabs:
    VERSION = 'v5'

    BASE_URL = f'https://api.peopledatalabs.com/{VERSION}'

    SAVE_DIRS = [  
        'company_enrichment',
        'company_search',
        'person_enrichment',
        'person_search',
    ]

    def __init__( self, api_key: str,) -> None:
        self.api_key = api_key
        self.create_dir()

    def create_dir(self):
        for dir in type(self).SAVE_DIRS:
            if not os.path.exists(dir):
                os.makedirs(dir)

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
        **kwargs
    ):
        # Checking minimum fields.
        required_fields = ['name', 'profile', 'ticker', 'website']

        if not any(i in required_fields for i in kwargs):
            raise ValueError(f'You need to specify at least one of {required_fields}')

        # Process to check if file company has already been enriched.
        if check_existing:
            existing_files = []
            existing_filenames =[f"company_enrichment/{i}" for i in os.listdir('company_enrichment')] 
            for file in existing_filenames:
                with open(file, 'r') as f:
                    file_data = json.loads(f.read())
                    existing_files.append(file_data)

            for existing_file in existing_files:
                for required_field in required_fields:
                    if required_field in kwargs:
                        if kwargs[required_field] in existing_file[required_field] or \
                        existing_file[required_field] in kwargs[required_field]:
                            print(f"{kwargs[required_field]} already exists.")
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
            if save:
                with open(os.path.join('company_enrichment', f"{json_response['id']}.json"), 'w') as out:
                    out.write(json.dumps(json_response))

        return json_response

    def search_company(
        self,
        required: str=None,
        strategy='AND',
        size=1,
        check_existing=True,
        verbose=True,
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

        if check_existing:
            existing = self.aggregate(dir_type='company_search')
            if existing is not None:
                existing = existing.website.tolist()
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

        if response['status'] == 200:
            for company in response['data']:
                id = company['id']
                with open(f'company_search/{id}.json', 'w') as out:
                    out.write(json.dumps(company))

        return response
    
    def search_person(
        self,
        required: str=None,
        strategy="AND",
        size=1,
        check_existing=True,
        verbose=True,
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

        if check_existing:
            existing = self.aggregate(dir_type='person_search')
            if existing is not None:
                existing = existing['work_email'].tolist()
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
            for company in response['data']:
                id = company['id']
                with open(f'person_search/{id}.json', 'w') as out:
                    out.write(json.dumps(company))

        return response

    def enrich_person(
        self,     
        min_likelihood: int=2,
        required=None,
        save=True,
        check_existing=True,
        **kwargs
    ):
        # Checking minimum fields.
        required_fields = ['profile', 'email', 'phone']

        if not any(i in required_fields for i in kwargs):
            raise ValueError(f'You need to specify at least one of {required_fields}')

        # Process to check if file company has already been enriched.
        if check_existing:
            existing_files = []
            existing_filenames =[f"person_enrichment/{i}" for i in os.listdir('person_enrichment')] 
            for file in existing_filenames:
                with open(file, 'r') as f:
                    file_data = json.loads(f.read())
                    existing_files.append(file_data)

            for existing_file in existing_files:
                if 'email' in kwargs:
                    if required == existing_file['work_email'] or required in existing_file['personal_emails']:
                        print(f"Email already exists.")
                        return None

                elif 'profile' in kwargs:
                    for profile in ['facebook_url', 'linkedin_url', 'twitter_url', 'github_url']:
                        if required == existing_file[profile]:
                            print(f"Profile already exists.")
                            return None

                elif 'phone' in kwargs:
                    if required== existing_file['mobile_phone'] or required in existing_file['phone_numbers']:
                        print(f"Phone already exists.")
                        return None

        url = f"{type(self).BASE_URL}/person/enrich"

        params = {
            "api_key": self.api_key,
            "min_likelihood": min_likelihood,
            "required": required
        }
        params.update(kwargs)

        json_response = requests.get(url, params=params).json()

        print(json_response)

        if json_response["status"] == 200:
            if save:
                with open(os.path.join('person_enrichment', f"{json_response['data']['id']}.json"), 'w') as out:
                    out.write(json.dumps(json_response['data']))

        return json_response
