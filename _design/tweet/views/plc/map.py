def map(doc):
    if doc['doc_type']=="Tweet" and doc.get('plc'):
        yield None, None
