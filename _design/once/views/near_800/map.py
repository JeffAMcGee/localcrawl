def map(doc):
    if doc['doc_type']=='Tweet':
        yield doc['uid'], int(doc['_id'][1:])
