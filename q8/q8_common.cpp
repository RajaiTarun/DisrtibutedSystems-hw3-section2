#include "q8_common.hpp"
#include <queue>
#include <iostream>
#include <iomanip>

// basically what this function done is that this takes a stats objects ref and the new upcoming measurement logs ref and then updates the stats accordingly
void updateStats(Stats &stats, const Measurement &m) {
    stats.count++;

    // updating temp stats
    stats.temp_sum += m.temperature;
    if (!stats.has_measurement || m.temperature < stats.temp_min) stats.temp_min = m.temperature;
    if (!stats.has_measurement || m.temperature > stats.temp_max) stats.temp_max = m.temperature;

    // updating humidity stats
    stats.humidity_sum += m.humidity;
    if (!stats.has_measurement || m.humidity < stats.humidity_min) stats.humidity_min = m.humidity;
    if (!stats.has_measurement || m.humidity > stats.humidity_max) stats.humidity_max = m.humidity;

// updating press stats
    stats.pressure_sum += m.pressure;
    if (!stats.has_measurement || m.pressure < stats.pressure_min) stats.pressure_min = m.pressure;
    if (!stats.has_measurement || m.pressure > stats.pressure_max) stats.pressure_max = m.pressure;

// updating rain stats
    stats.rainfall_sum += m.rainfall;
    if (!stats.has_measurement || m.rainfall > stats.rainfall_max) stats.rainfall_max = m.rainfall;

// updating wind stats
    stats.wind_sum += m.wind_speed;
    if (!stats.has_measurement || m.wind_speed > stats.wind_max) stats.wind_max = m.wind_speed;

// updating extreme temp stats
    if (m.temperature >= 40.0 || m.temperature <= 0.0) {
        stats.extreme_count++;
    }

// maintaing the hottest and the coldest per station
    if (!stats.has_measurement || isHotter(m, stats.hottest)) {
        stats.hottest = m;
    }

    if (!stats.has_measurement || isColder(m, stats.coldest)) {
        stats.coldest = m;
    }

    // basically what we are doing is that we are maintain per station info in each process, so at the end we can just combine all of them using master and then we can get info of all the stations from all the worker processes
    stats.stations[m.station_id].count++;
    stats.stations[m.station_id].temp_sum += m.temperature;
    stats.stations[m.station_id].rainfall_sum += m.rainfall;

    // in this we find the interval id and increment the freq count of that interval id
    long long interval_id = m.timestamp / 60;
    stats.intervals[interval_id]++;

    // just a bool flag to indicate that this processes worker has processed some measurement, will be needed for the min_max type things
    stats.has_measurement = true;
}

// just a helper function that will handle returning the hottest temp following the tie break rules
bool isHotter(const Measurement &a, const Measurement &b) {
    if (a.temperature != b.temperature) return a.temperature > b.temperature;
    if (a.timestamp != b.timestamp) return a.timestamp < b.timestamp;
    return a.station_id < b.station_id;
}

// just a helper function that will handle returning the hottest temp following the tie break rules
bool isColder(const Measurement &a, const Measurement &b) {
    if (a.temperature != b.temperature) return a.temperature < b.temperature;
    if (a.timestamp != b.timestamp) return a.timestamp < b.timestamp;
    return a.station_id < b.station_id;
}

// compares stations based on their measurement counts
bool isBetterStation(int a_id, const StationStats &a, int b_id, const StationStats &b) {
    if (a.count != b.count) return a.count > b.count;
    return a_id < b_id;
}

// basically once we are done with each process doing its work now we need to merge them inorder to get final answer
void mergingWorkerProcessStats(Stats &dest, const Stats &source) {
    if (!source.has_measurement) return;

    if (!dest.has_measurement) {
        dest = source;
        return;
    }

    dest.count += source.count;

    // temperature
    dest.temp_sum += source.temp_sum;
    if (source.temp_min < dest.temp_min) dest.temp_min = source.temp_min;
    if (source.temp_max > dest.temp_max) dest.temp_max = source.temp_max;

    // humidity
    dest.humidity_sum += source.humidity_sum;
    if (source.humidity_min < dest.humidity_min) dest.humidity_min = source.humidity_min;
    if (source.humidity_max > dest.humidity_max) dest.humidity_max = source.humidity_max;

    // pressure
    dest.pressure_sum += source.pressure_sum;
    if (source.pressure_min < dest.pressure_min) dest.pressure_min = source.pressure_min;
    if (source.pressure_max > dest.pressure_max) dest.pressure_max = source.pressure_max;

    // rainfall
    dest.rainfall_sum += source.rainfall_sum;
    if (source.rainfall_max > dest.rainfall_max) dest.rainfall_max = source.rainfall_max;

    // wind speed
    dest.wind_sum += source.wind_sum;
    if (source.wind_max > dest.wind_max) dest.wind_max = source.wind_max;

    // extreme temperature events
    dest.extreme_count += source.extreme_count;

    // hottest and coldest
    if (isHotter(source.hottest, dest.hottest)) dest.hottest = source.hottest;
    if (isColder(source.coldest, dest.coldest)) dest.coldest = source.coldest;

    // stats for each station
    for (int i = 0; i < dest.stations.size(); i++) {
        dest.stations[i].count += source.stations[i].count;
        dest.stations[i].temp_sum += source.stations[i].temp_sum;
        dest.stations[i].rainfall_sum += source.stations[i].rainfall_sum;
    }

    // interval stats
    for (const auto &i : source.intervals) {
        dest.intervals[i.first] += i.second;
    }
}

// we are distributing the work to distributed processes using this function
Stats computeRangeOnWorker(const vector<Measurement> &data, int begin, int end, int S) {
    Stats result(S);

    for (int i = begin; i < end; i++) {
        updateStats(result, data[i]);
    }

    return result;
}

// this function gives us the partition that the worker with process number P should work on
pair<int, int> getPartition(int worker, int P, int N) {
    int chunk = N / P;
    int remainder = N % P;

    int begin, end;
    if (worker < remainder) {
        begin = worker * (chunk + 1);
        end = begin + (chunk + 1);
    } else {
        begin = remainder * (chunk + 1) + (worker - remainder) * chunk;
        end = begin + chunk;
    }

    return {begin, end};
}

// P is the total number of processes. 1 is master and the rest P-1 are workers
Stats runMasterWorkerSimulation(const vector<Measurement> &data, int N, int S, int P) {
    int numWorkers = P - 1;

    Stats globalStats(S);

    for (int worker = 0; worker < numWorkers; worker++) {
        auto [begin, end] = getPartition(worker, numWorkers, N);

        Stats localStats = computeRangeOnWorker(data, begin, end, S);

        mergingWorkerProcessStats(globalStats, localStats);
    }

    return globalStats;
}

// the min heap logic that we will apply after we have processed all the logs, one imp thing is that first our approach was to go for all the distributed processes having k size min heap and then master using all that k size min heaps forms a new k size min heap, but the issue is that we can have data distributed among distributed processes so we will need to first process all the logs and then find the top k

struct CompareStations {
    const vector<StationStats>& stations;

    bool operator()(int a, int b) const {
        return isBetterStation(
            a, stations[a],
            b, stations[b]
        );
    }
};
vector<TopStationEntry> getTopStations(const Stats &globalStats, int K) {
    CompareStations compare{globalStats.stations};

    priority_queue<int, vector<int>, CompareStations> minHeap(compare);

    for (int id = 0; id < (int)globalStats.stations.size(); id++) {
        if (globalStats.stations[id].count == 0) continue;

        if ((int)minHeap.size() < K) {
            minHeap.push(id);
        } else if (!minHeap.empty() && isBetterStation(id, globalStats.stations[id], minHeap.top(), globalStats.stations[minHeap.top()])) {
            minHeap.pop();
            minHeap.push(id);
        }
    }

    // popping a min-heap gives worst-to-best order, so collect then reverse
    vector<int> ids;
    while (!minHeap.empty()) {
        ids.push_back(minHeap.top());
        minHeap.pop();
    }
    reverse(ids.begin(), ids.end());

    vector<TopStationEntry> result;
    for (int id : ids) {
        const StationStats &s = globalStats.stations[id];

        TopStationEntry entry;
        entry.station_id = id;
        entry.count = s.count;
        entry.avg_temperature = s.temp_sum / s.count;
        entry.total_rainfall = s.rainfall_sum;

        result.push_back(entry);
    }

    return result;
}

// helper function to get final averages for output
FinalAverages computeFinalAverages(const Stats &globalStats) {
    FinalAverages result;
    result.avg_temperature = globalStats.temp_sum / globalStats.count;
    result.avg_humidity = globalStats.humidity_sum / globalStats.count;
    result.avg_pressure = globalStats.pressure_sum / globalStats.count;
    result.avg_wind_speed = globalStats.wind_sum / globalStats.count;
    return result;
}

// call only after all worker Stats have been merged into globalStats,
// since one interval's measurements can be spread across multiple workers
BusiestInterval getBusiestInterval(const Stats &globalStats) {
    BusiestInterval best;
    best.interval_id = 0;
    best.count = 0;
    bool found = false;

    for (const auto &entry : globalStats.intervals) {
        long long interval_id = entry.first;
        long long count = entry.second;

        if (!found || count > best.count || (count == best.count && interval_id < best.interval_id)) {
            best.interval_id = interval_id;
            best.count = count;
            found = true;
        }
    }

    return best;
}

void printResults(const Stats &globalStats, int K) {
    // N=0 edge case: no records means no meaningful average/min/max/hottest/
    // coldest/busiest-interval value exists, so print only the one field
    // that is always well-defined and stop.
    if (globalStats.count == 0) {
        cout << "TOTAL_MEASUREMENTS 0\n";
        return;
    }

    FinalAverages avg = computeFinalAverages(globalStats);
    BusiestInterval busiest = getBusiestInterval(globalStats);
    vector<TopStationEntry> topStations = getTopStations(globalStats, K);

    cout << fixed << setprecision(6);

    cout << "TOTAL_MEASUREMENTS " << globalStats.count << "\n";

    cout << "AVERAGE_TEMPERATURE " << avg.avg_temperature << "\n";
    cout << "MIN_TEMPERATURE " << globalStats.temp_min << "\n";
    cout << "MAX_TEMPERATURE " << globalStats.temp_max << "\n";

    cout << "AVERAGE_HUMIDITY " << avg.avg_humidity << "\n";
    cout << "MIN_HUMIDITY " << globalStats.humidity_min << "\n";
    cout << "MAX_HUMIDITY " << globalStats.humidity_max << "\n";

    cout << "AVERAGE_PRESSURE " << avg.avg_pressure << "\n";
    cout << "MIN_PRESSURE " << globalStats.pressure_min << "\n";
    cout << "MAX_PRESSURE " << globalStats.pressure_max << "\n";

    cout << "TOTAL_RAINFALL " << globalStats.rainfall_sum << "\n";
    cout << "MAX_RAINFALL " << globalStats.rainfall_max << "\n";

    cout << "AVERAGE_WIND_SPEED " << avg.avg_wind_speed << "\n";
    cout << "MAX_WIND_SPEED " << globalStats.wind_max << "\n";

    cout << "EXTREME_TEMPERATURE_EVENTS " << globalStats.extreme_count << "\n";

    cout << "HOTTEST_MEASUREMENT " << globalStats.hottest.temperature << " "
         << globalStats.hottest.station_id << " " << globalStats.hottest.timestamp << "\n";

    cout << "COLDEST_MEASUREMENT " << globalStats.coldest.temperature << " "
         << globalStats.coldest.station_id << " " << globalStats.coldest.timestamp << "\n";

    cout << "BUSIEST_INTERVAL " << busiest.interval_id << " " << busiest.count << "\n";

    cout << "TOP_STATIONS\n";
    for (const auto &s : topStations) {
        cout << s.station_id << " " << s.count << " " << s.avg_temperature << " " << s.total_rainfall << "\n";
    }
}

// the correctness oracle: single pass over all N records, no partitioning
Stats computeDirectBaseline(const vector<Measurement> &data, int N, int S) {
    return computeRangeOnWorker(data, 0, N, S);
}