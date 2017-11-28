from au.com.gegroup.ts.utils import *
# testing date to long conversion
date_ms = date_to_long("2017-10-01")
assert date_ms == 1506816000000

# testing get_tags_filter
tags_filter = get_tags_filter("supply and water and temp and sensor")
# print(tags_filter)
assert tags_filter == "supply = '1' and water = '1' and temp = '1' and sensor = '1'"

# testing date_filter
date_filter = get_datetime_filter(date_to_long("2017-10-10"), date_to_long("2017-10-11"))
# print(date_filter)
assert date_filter == "datetime >= 1507593600000 and datetime <= 1507680000000"

swt = get_query(date_filter, tags_filter, "swt")
print(swt)

query = read("lwt", "2017-10-10", "2017-10-11", "return and water and temp and sensor")
print(query)




