import os
import json

import requests



class PeopleDataLabs:
    VERSION = 'v5'
    BASE_URL = f'https://api.peopledatalabs.com/{VERSION}'

    REQUIRED_FIELDS = [
        "birth_date", "education", "emails",
        "experience", "facebook_id", "facebook_username",
        "full_name", "gender", "github_username",
        "industry", "interests", "job_company_name",
        "job_title", "last_name", "linkedin_id",
        "linkedin_username", "location_country", "location_locality",
        "location_name", "location_postal_code", "location_region",
        "location_street_address", "mobile_phone", "personal_emails",
        "phone_numbers", "profiles", "skills",
        "twitter_username", "work_email",
    ]

    INPUT_FIELDS = [
        "name",
        "first_name",
        "last_name",
        "middle_name",
        "location",
        "street_address",
        "locality",
        "region",
        "country",
        "postal_code",
        "company",
        "school",
        "phone",
        "email",
        "email_hash",
        "profile",
        "lid",
        "birth_date",
    ]

    def __init__(
        self,
        api_key: str,
        check_saved=True
    ) -> None:
        self.api_key = api_key
        self.save_dir = '_data'

        self.check_saved = check_saved
        self.create_dir()

    def create_dir(self):
        if self.check_saved:
            if not os.path.exists(self.save_dir):
                os.makedirs(self.save_dir)

    def enrich_person(
        self,
        min_likelihood: int=2,
        required=None,
        save=True,
        **kwargs
    ):
        '''
        Minimum info
        ###############3
        profile OR email OR phone OR email_hash OR lid OR ( 
        (
            (first_name AND last_name) OR name) AND 
            (locality OR region OR company OR school OR location OR postal_code)
        )
        '''
        # Check required if provided
        if required and required not in type(self).REQUIRED_FIELDS:
            raise ValueError('Required field not allowed. Check PeopleDataLabs.REQUIRED_FIELDS.')

        # Check if kwargs are valid.
        if not all(elem in type(self).INPUT_FIELDS  for elem in kwargs):
            raise ValueError('One or more fields not allowed. Check PeopleDataLabs.INPUT_FIELDS')
    
        url = f"{type(self).BASE_URL}/person/enrich"

        params = {
            "api_key": self.api_key,
            "min_likelihood": min_likelihood,
            "required": required

        } 
        params.update(kwargs)

        json_response = requests.get(url, params=params).json()

        if json_response["status"] == 200:
            record = json_response['data']

            if save:
                with open(os.path.join(self.save_dir, f"{record['id']}.json"), 'w') as out:
                    out.write(json.dumps(record))

        return json_response
