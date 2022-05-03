'''
Test stuff
'''
df


sf.upsert(
	tablename='Account',
	dataframe=df,
	update=True,
	insert=False,
	conflict_on='Id',
	return_response=False,
	verbose=False
	)

