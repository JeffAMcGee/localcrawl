def map(doc):
    yield doc['uid'], [doc['_id'], doc['ca']]
