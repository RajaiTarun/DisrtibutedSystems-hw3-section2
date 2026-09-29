"""
The Q8 weather analytics in Python: a port of HW2's q8_common.cpp.

A Stats object keeps the running statistics of some measurements (the same fields as HW2's Stats
struct). Two Stats objects can be merged, which is how the coordinator combines the workers' results.

A measurement is a tuple:
    (timestamp, station_id, temperature, humidity, pressure, rainfall, wind_speed)

Tie-break rules (same as HW2):
    hottest / coldest : higher / lower temperature, then smaller timestamp, then smaller station id
    top-K stations    : more measurements first, then smaller station id
    busiest interval  : more measurements first, then smaller interval id (interval = timestamp // 60)
"""

# positions inside a measurement tuple
TIMESTAMP, STATION, TEMP, HUM, PRES, RAIN, WIND = range(7)


def is_hotter(a, b):
    """a and b are (temperature, timestamp, station_id)"""
    if a[0] != b[0]:
        return a[0] > b[0]
    if a[1] != b[1]:
        return a[1] < b[1]
    return a[2] < b[2]


def is_colder(a, b):
    """a and b are (temperature, timestamp, station_id)"""
    if a[0] != b[0]:
        return a[0] < b[0]
    if a[1] != b[1]:
        return a[1] < b[1]
    return a[2] < b[2]


class Stats:
    def __init__(self, S):
        self.S = S
        self.count = 0
        self.temp_sum = 0.0
        self.temp_min = 0.0
        self.temp_max = 0.0
        self.humidity_sum = 0.0
        self.humidity_min = 0.0
        self.humidity_max = 0.0
        self.pressure_sum = 0.0
        self.pressure_min = 0.0
        self.pressure_max = 0.0
        self.rainfall_sum = 0.0
        self.rainfall_max = 0.0
        self.wind_sum = 0.0
        self.wind_max = 0.0
        self.extreme_count = 0
        # hottest / coldest measurement as (temperature, timestamp, station_id)
        self.hottest = None
        self.coldest = None
        # per station (index = station id): number of measurements, temperature sum, rainfall sum
        self.station_count = [0] * S
        self.station_temp = [0.0] * S
        self.station_rain = [0.0] * S
        # interval id -> number of measurements
        self.intervals = {}
        # the busiest interval so far (interval_id, count), kept up to date by update(), so that a
        # query does not have to scan all intervals. None = unknown (after merge), then we scan
        self.best_interval = None

    def has_measurement(self):
        return self.count > 0

    def update(self, m):
        """adds one measurement (same steps as HW2's updateStats)"""
        timestamp, station, temp, hum, pres, rain, wind = m
        first = self.count == 0
        self.count += 1

        self.temp_sum += temp
        if first or temp < self.temp_min: self.temp_min = temp
        if first or temp > self.temp_max: self.temp_max = temp

        self.humidity_sum += hum
        if first or hum < self.humidity_min: self.humidity_min = hum
        if first or hum > self.humidity_max: self.humidity_max = hum

        self.pressure_sum += pres
        if first or pres < self.pressure_min: self.pressure_min = pres
        if first or pres > self.pressure_max: self.pressure_max = pres

        self.rainfall_sum += rain
        if first or rain > self.rainfall_max: self.rainfall_max = rain

        self.wind_sum += wind
        if first or wind > self.wind_max: self.wind_max = wind

        if temp >= 40.0 or temp <= 0.0:
            self.extreme_count += 1

        candidate = (temp, timestamp, station)
        if first or is_hotter(candidate, self.hottest): self.hottest = candidate
        if first or is_colder(candidate, self.coldest): self.coldest = candidate

        self.station_count[station] += 1
        self.station_temp[station] += temp
        self.station_rain[station] += rain

        interval = timestamp // 60
        c = self.intervals.get(interval, 0) + 1
        self.intervals[interval] = c
        # counts only go up, so only the interval that just changed can become the new busiest one
        # (same rule as HW2: more measurements, then the smaller interval id)
        best = self.best_interval
        if first:
            self.best_interval = (interval, c)
        elif best is not None and (c > best[1] or (c == best[1] and interval < best[0])):
            self.best_interval = (interval, c)

    def merge(self, other):
        """adds another Stats into this one (same steps as HW2's mergingWorkerProcessStats)"""
        if other.count == 0:
            return
        first = self.count == 0
        self.count += other.count

        self.temp_sum += other.temp_sum
        if first or other.temp_min < self.temp_min: self.temp_min = other.temp_min
        if first or other.temp_max > self.temp_max: self.temp_max = other.temp_max

        self.humidity_sum += other.humidity_sum
        if first or other.humidity_min < self.humidity_min: self.humidity_min = other.humidity_min
        if first or other.humidity_max > self.humidity_max: self.humidity_max = other.humidity_max

        self.pressure_sum += other.pressure_sum
        if first or other.pressure_min < self.pressure_min: self.pressure_min = other.pressure_min
        if first or other.pressure_max > self.pressure_max: self.pressure_max = other.pressure_max

        self.rainfall_sum += other.rainfall_sum
        if first or other.rainfall_max > self.rainfall_max: self.rainfall_max = other.rainfall_max

        self.wind_sum += other.wind_sum
        if first or other.wind_max > self.wind_max: self.wind_max = other.wind_max

        self.extreme_count += other.extreme_count

        if first or is_hotter(other.hottest, self.hottest): self.hottest = other.hottest
        if first or is_colder(other.coldest, self.coldest): self.coldest = other.coldest

        for i in range(min(self.S, other.S)):
            self.station_count[i] += other.station_count[i]
            self.station_temp[i] += other.station_temp[i]
            self.station_rain[i] += other.station_rain[i]

        for interval, c in other.intervals.items():
            self.intervals[interval] = self.intervals.get(interval, 0) + c
        self.best_interval = None   # has to be found again by scanning (see busiest_interval)

    def busiest_interval(self):
        """(interval_id, count): most measurements, ties -> smaller interval id"""
        if self.best_interval is not None:
            return self.best_interval   # kept up to date by update(), no scan needed
        best_id, best_count = 0, 0
        found = False
        for interval, c in self.intervals.items():
            if not found or c > best_count or (c == best_count and interval < best_id):
                best_id, best_count = interval, c
                found = True
        return best_id, best_count

    def top_stations(self, K):
        """list of (station_id, count, average_temperature, total_rainfall), best first"""
        ids = [i for i in range(self.S) if self.station_count[i] > 0]
        # more measurements first, then smaller station id
        ids.sort(key=lambda i: (-self.station_count[i], i))
        return [(i, self.station_count[i], self.station_temp[i] / self.station_count[i], self.station_rain[i])
                for i in ids[:max(K, 0)]]

    def format_results(self, K):
        """the analytics as text, in exactly the HW2 output format (6 decimals)"""
        if self.count == 0:
            return "TOTAL_MEASUREMENTS 0\n"
        n = self.count
        busiest_id, busiest_count = self.busiest_interval()
        lines = [
            f"TOTAL_MEASUREMENTS {n}",
            f"AVERAGE_TEMPERATURE {self.temp_sum / n:.6f}",
            f"MIN_TEMPERATURE {self.temp_min:.6f}",
            f"MAX_TEMPERATURE {self.temp_max:.6f}",
            f"AVERAGE_HUMIDITY {self.humidity_sum / n:.6f}",
            f"MIN_HUMIDITY {self.humidity_min:.6f}",
            f"MAX_HUMIDITY {self.humidity_max:.6f}",
            f"AVERAGE_PRESSURE {self.pressure_sum / n:.6f}",
            f"MIN_PRESSURE {self.pressure_min:.6f}",
            f"MAX_PRESSURE {self.pressure_max:.6f}",
            f"TOTAL_RAINFALL {self.rainfall_sum:.6f}",
            f"MAX_RAINFALL {self.rainfall_max:.6f}",
            f"AVERAGE_WIND_SPEED {self.wind_sum / n:.6f}",
            f"MAX_WIND_SPEED {self.wind_max:.6f}",
            f"EXTREME_TEMPERATURE_EVENTS {self.extreme_count}",
            f"HOTTEST_MEASUREMENT {self.hottest[0]:.6f} {self.hottest[2]} {self.hottest[1]}",
            f"COLDEST_MEASUREMENT {self.coldest[0]:.6f} {self.coldest[2]} {self.coldest[1]}",
            f"BUSIEST_INTERVAL {busiest_id} {busiest_count}",
            "TOP_STATIONS",
        ]
        for station, c, avg_temp, rain in self.top_stations(K):
            lines.append(f"{station} {c} {avg_temp:.6f} {rain:.6f}")
        return "\n".join(lines) + "\n"

    # ---------------- conversion to / from the protobuf message PartialStats ----------------

    def to_proto(self, send_all_intervals):
        """this Stats as a weather_pb2.PartialStats message.
        send_all_intervals=False: only this worker's busiest interval is sent (interval strategy:
        the worker has all records of its intervals, so its busiest interval is final)"""
        import weather_pb2
        p = weather_pb2.PartialStats(
            has_measurement=self.count > 0, count=self.count,
            temp_sum=self.temp_sum, temp_min=self.temp_min, temp_max=self.temp_max,
            humidity_sum=self.humidity_sum, humidity_min=self.humidity_min, humidity_max=self.humidity_max,
            pressure_sum=self.pressure_sum, pressure_min=self.pressure_min, pressure_max=self.pressure_max,
            rainfall_sum=self.rainfall_sum, rainfall_max=self.rainfall_max,
            wind_sum=self.wind_sum, wind_max=self.wind_max, extreme_count=self.extreme_count)
        if self.count > 0:
            for field, value in ((p.hottest, self.hottest), (p.coldest, self.coldest)):
                field.temperature, field.timestamp, field.station_id = value
        p.stations.extend(weather_pb2.StationPartial(count=c, temp_sum=t, rain_sum=r)
                          for c, t, r in zip(self.station_count, self.station_temp, self.station_rain))
        if send_all_intervals:
            p.intervals.extend(weather_pb2.IntervalCount(interval_id=i, count=c) for i, c in self.intervals.items())
        elif self.count > 0:
            best_id, best_count = self.busiest_interval()
            p.intervals.append(weather_pb2.IntervalCount(interval_id=best_id, count=best_count))
        return p

    @staticmethod
    def from_proto(p, S):
        """a weather_pb2.PartialStats message as a Stats object"""
        st = Stats(S)
        st.count = p.count
        st.temp_sum, st.temp_min, st.temp_max = p.temp_sum, p.temp_min, p.temp_max
        st.humidity_sum, st.humidity_min, st.humidity_max = p.humidity_sum, p.humidity_min, p.humidity_max
        st.pressure_sum, st.pressure_min, st.pressure_max = p.pressure_sum, p.pressure_min, p.pressure_max
        st.rainfall_sum, st.rainfall_max = p.rainfall_sum, p.rainfall_max
        st.wind_sum, st.wind_max = p.wind_sum, p.wind_max
        st.extreme_count = p.extreme_count
        if p.count > 0:
            st.hottest = (p.hottest.temperature, p.hottest.timestamp, p.hottest.station_id)
            st.coldest = (p.coldest.temperature, p.coldest.timestamp, p.coldest.station_id)
        for i, s in enumerate(p.stations):
            if i >= S:
                break
            st.station_count[i] = s.count
            st.station_temp[i] = s.temp_sum
            st.station_rain[i] = s.rain_sum
        for ic in p.intervals:
            st.intervals[ic.interval_id] = st.intervals.get(ic.interval_id, 0) + ic.count
        return st


def read_dataset(path):
    """reads a Q8 input file. returns (has_header, N, K, S, list of measurement tuples).
    like HW2: the first line is 'N K S'; an empty file has no header"""
    with open(path) as f:
        data = f.read().split()
    if len(data) < 3:
        return False, 0, 0, 0, []
    N, K, S = int(data[0]), int(data[1]), int(data[2])
    records = []
    values = data[3:3 + 7 * N]
    for i in range(0, len(values) - 6, 7):
        records.append((int(values[i]), int(values[i + 1]), float(values[i + 2]), float(values[i + 3]),
                        float(values[i + 4]), float(values[i + 5]), float(values[i + 6])))
    return True, N, K, S, records
