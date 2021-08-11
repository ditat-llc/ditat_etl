from setuptools import setup, find_packages


setup(
	author='ditat.io',
	description='Multiple tools and utilities for ETL pipelines and others.',
	name='ditat_etl',
	version='0.0.2',
	packages=find_packages(include=['ditat_etl', 'ditat_etl.*']),
	python_requires='>=3.7',
	install_requires=[
		'pandas',
		'psycopg2-binary'
	]

)
