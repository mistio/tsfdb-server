import fdb
import fdb.tuple
from datetime import datetime
from tsfdb_server_v1.controllers.helpers import create_key_tuple_second, \
    create_key_tuple_minute, create_key_tuple_hour, create_key_tuple_day
fdb.api_version(620)


def validate_aggregation(start, stop):
    db = fdb.open()
    if fdb.directory.exists(db, "monitoring"):
        monitoring = fdb.directory.open(db, "monitoring")

    minute_dir = monitoring.open(db, ('metric_per_minute',))
    hour_dir = monitoring.open(db, ('metric_per_hour',))
    day_dir = monitoring.open(db, ('metric_per_day',))

    metric = "system.load1"
    machine = "123456789"
    dt = datetime.fromtimestamp(start)
    day, hour, minute = dt.day, dt.hour, dt.minute
    sum_day, sum_hour, sum_minute = 0, 0, 0
    count_day, count_hour, count_minute = 0, 0, 0
    delta = 0
    # Check if the summarizations are correct
    for k, v in db[monitoring.pack(create_key_tuple_second(
            datetime.fromtimestamp(start), machine, metric)):
            monitoring.pack(create_key_tuple_second(
            datetime.fromtimestamp(start+stop), machine, metric))]:
        # print(monitoring.unpack(k), fdb.tuple.unpack(v))
        current_key = monitoring.unpack(k)
        value = fdb.tuple.unpack(v)[0]
        # print(current_key, value)
        # print(sum_minute, count_minute)
        if day < current_key[4]:
            sum_tuple = fdb.tuple.unpack(db[day_dir.pack(create_key_tuple_day(
                datetime.fromtimestamp(start + delta - 5), machine, metric))])
            assert sum_day == sum_tuple[0]
            assert count_day == sum_tuple[1]
            sum_day = 0
            count_day = 0
            day += 1

        if hour < current_key[5]:
            sum_tuple = fdb.tuple.unpack(db[hour_dir.pack(
                create_key_tuple_hour(
                    datetime.fromtimestamp(start + delta - 5),
                    machine, metric))])
            assert sum_hour == sum_tuple[0]
            assert count_hour == sum_tuple[1]
            sum_hour = 0
            count_hour = 0
            hour += 1

        if minute < current_key[6]:
            sum_tuple = fdb.tuple.unpack(db[minute_dir.pack(
                create_key_tuple_minute(
                    datetime.fromtimestamp(start + delta - 5),
                    machine, metric))])
            # print(create_key_tuple_minute(
            #    datetime.fromtimestamp(start + delta - 5), machine, metric))
            # print(sum_tuple)
            # print(sum_minute / count_minute, sum_tuple[0] / sum_tuple[1])
            # print(sum_minute, count_minute, "====",
            #      sum_tuple[0], sum_tuple[1])
            assert sum_minute == sum_tuple[0]
            assert count_minute == sum_tuple[1]
            sum_minute = 0
            count_minute = 0
            minute += 1

        sum_day += value
        sum_hour += value
        sum_minute += value
        count_day += 1
        count_hour += 1
        count_minute += 1
        delta += 5
