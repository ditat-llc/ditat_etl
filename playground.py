'''
Test stuff
'''

from ditat_etl.utils.search import EntitySearch


es = EntitySearch('data.csv', th=0.2)

p = {
	'ben_wells': {
		'title': [
			'chief financial officer', 'chief executive officer', 'ceo', 'cfo',
			'founder', 'controller', 'president', 'office manager'
		],
		'billingstate': 'washington',
		'naics_industry_title__c': 'construction' 

	},
	'ben_wells2': {
		'title': [
			'chief financial officer', 'chief executive officer', 'ceo', 'cfo',
			'founder', 'controller', 'president', 'office manager'
		],
		'billingstate': 'washington',
		'naics_industry_title__c': 'construction' 

	},
	'kaveh_karimian': {
		'billingstate': 'california',
		'title': ['human resources'],
		'numberofemployees': [50, 250],
	},
	'michael_sinatra': {
		'title':  ['ceo', 'cfo'],
		'billingstate': 'florida',
		'numberofemployees': [10, None],
	}
}

r = es.distribute(p, size=5, repetition=False)

for i, j in r.items():
	print(i)
	print(j[[
		'firstname', 'lastname', 'title', 'billingstate',
		'naics_industry_title__c', 'numberofemployees'
	]])
