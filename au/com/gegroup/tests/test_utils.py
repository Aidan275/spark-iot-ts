from au.com.gegroup.ts.datetime.utils import *
# testing date to long conversion
date_ms = date_to_long("2017-10-01")
assert date_ms == 1506816000000

print(last(pd.Timedelta('1 days')))

print("yesterday")
print(yesterday())
print(date_to_long(yesterday()[0]), date_to_long(yesterday()[1]))

print("this_month")
print(this_month())

print("last_month")
print(last_month())
print(as_long_tuple(last_month()))

print("test as_long_tuple")
print(as_long_tuple((1511827200000, 1511913599999)))
