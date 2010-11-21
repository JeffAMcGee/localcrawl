def map(doc):
    if doc['doc_type']=='Tweet':
        yield doc['uid'], [doc['_id'], doc['ca']]
