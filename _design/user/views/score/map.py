def map(doc):
    if doc['doc_type']=='User' and 'rfs' in doc and 'ats' in doc:
        from math import log
        avg = (doc['rfs'] + doc['ats']) /2.0
	yield int(log(max(avg,1),2)),None
