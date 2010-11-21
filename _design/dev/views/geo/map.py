def map(doc):
    if doc['doc_type']=="Tweet" and doc.get('geo'):
        yield None, None
