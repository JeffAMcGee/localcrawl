def reduce(keys, values, rereduce):
    cutoff = 8000000000000000L
    if rereduce:
        before = [v[0] for v in values if v[0] is not None]
        after =  [v[1] for v in values if v[1] is not None]
    else:
        before = [v for v in values if v<cutoff]
        after = [v for v in values if v>cutoff]
    return max(before) if before else None, min(after) if after else None

