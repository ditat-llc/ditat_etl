'''
Test stuff
'''

from ditat_etl.utils.entity_resolution import StateStandard


us = StateStandard()
term = 'co'

r = us.classify(text=term)
print(r)

