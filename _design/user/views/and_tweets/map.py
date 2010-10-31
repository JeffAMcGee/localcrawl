def fun(doc):
    if doc['doc_type']=='User':
        yield [doc['_id'],0], None
    if doc['doc_type']=='Tweet':
        yield [doc['uid'],1], None
