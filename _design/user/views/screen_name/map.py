def fun(doc):
  if doc['doc_type']=='User' and doc.get('sn'):
    yield doc['sn'], None
