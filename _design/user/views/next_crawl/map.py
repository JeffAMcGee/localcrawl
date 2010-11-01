def map(doc):
    if doc['doc_type']=='User' and 'ncd' in doc.get('l',[]):
        yield doc['l']['ncd'], None
