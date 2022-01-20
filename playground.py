'''
Test stuff
'''

from ditat_etl.utils.entity_resolution import NaicsStandard

n = NaicsStandard()

term = [
    'Non profit'
]
r = n.classify(text=term, n=5)
print(r)
