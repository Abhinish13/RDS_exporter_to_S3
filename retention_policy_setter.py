
def retention_days(n):
    retentionInDays = [
        1,
        3,
        5,
        7,
        14,
        30,
        60,
        90,
        120,
        150,
        180,
        365,
        400,
        545,
        731,
        1827,
        3653,
    ]
    for retention_day in retentionInDays:
        if n < retention_day:
            return retention_day
