'''
Test stuff
'''

from ditat_etl.utils.entity_resolution import NaicsStandard

n = NaicsStandard()

term = ['burgers and fries', 'government', 'government', 'labor']
r = n.classify(text=term)
print(r)

