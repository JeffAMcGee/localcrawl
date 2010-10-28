def map(doc):
    if doc['doc_type']=="Tweet":
        yield doc.get('uid'), None
