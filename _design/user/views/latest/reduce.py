def reduce(keys, values, rereduce):
    return max(values, key=lambda v:int(v[0][1:]))
