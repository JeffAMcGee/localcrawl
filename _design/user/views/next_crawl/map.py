def map(doc):
    if doc['doc_type']=='User' and 'ncd' in doc:
        yield doc['ncd'], None
