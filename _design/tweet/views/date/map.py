def map(doc):
    if doc['doc_type']=="Tweet" and doc.get('ca'):
        yield doc['ca'][0:3], None
