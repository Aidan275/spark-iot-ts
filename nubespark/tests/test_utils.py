from nubespark.ts.datetime.utils import *
# testing date to long conversion
from nubespark.ts.reader import Reader

date_ms = date_to_long("2017-10-01")
print(date_ms)
assert date_ms == 1506816000000000000

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

print("today = ", Reader.process_string_date("today"))
print("yesterday = ", Reader.process_string_date("yesterday"))
print("past_1days = ", Reader.process_string_date("past_1days"))
print("last_month = ", Reader.process_string_date("last_month"))
print("this_month = ", Reader.process_string_date("this_month"))
print("2018-01-01,2018-04-01 = ", Reader.process_string_date("2018-01-01,2018-04-01"))
print("2018-01-01T00:00:00,2018-04-01T23:59:59 = ", Reader.process_string_date("2018-01-01T00:00:00,2018-04-01T23:59:59"))
print("2018-04-01 = ", Reader.process_string_date("2018-04-01"))
print("2018-01-01T00:00:00 = ", Reader.process_string_date("2018-01-01T00:00:00"))
