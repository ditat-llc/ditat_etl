from setuptools import setup, find_packages
from ditat_etl import __version__

with open('README.md', 'r') as f:
	long_description = f.read()

setup(
	author='ditat.io',
	url='https://github.com/ditat-llc/ditat_etl',
	author_email='tomas@ditat.io',
	description='Multiple tools and utilities for ETL pipelines and others.',
	long_description=long_description,
	long_description_content_type='text/markdown',
	name='ditat_etl',
	version=__version__,
	packages=find_packages(include=['ditat_etl', 'ditat_etl.*']),
	include_package_data=True,
	package_data={
		'ditat_etl': [
			'url/proxies.json',
			'utils/country_codes.csv',
			'utils/us_states.csv',
			'utils/entity_resolution/domains_ignored.txt',
			'url/domains_ignored.txt',
            'utils/entity_resolution/naics.csv'
		]
	},
	python_requires='>=3.7',
	install_requires=[
		'pandas',
		'psycopg2-binary',
		'requests',
		'pysocks',
		'selenium',
		'phonenumbers',
        'boto3',
        'simple-salesforce',
        'spacy'
	]

)
