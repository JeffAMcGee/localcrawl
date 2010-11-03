def map(doc):
    if doc['doc_type']=='User':
        from math import log
        avg = (doc['l']['rfs'] + doc['l']['ats']) /2.0
	yield int(log(max(avg,1),2)),None
