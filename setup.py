from setuptools import setup, find_packages

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
	version='0.0.4',
	packages=find_packages(include=['ditat_etl', 'ditat_etl.*']),
	python_requires='>=3.7',
	install_requires=[
		'pandas',
		'psycopg2-binary'
	]

)
