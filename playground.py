'''
Test stuff
'''


import pandas as pd

codes = pd.read_csv('./ditat_etl/utils/country_codes.csv')
codes.set_index('country', inplace=True)

df = pd.DataFrame({'new': ['chile', 'australia']})

df['new_column'] = df['new'].map(codes['iso'])
print(df)
