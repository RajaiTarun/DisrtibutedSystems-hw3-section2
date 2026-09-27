// reducer for q8 using mapreduce
// it gets the (sorted) output of all the combiners together and produces the final q8 output
//
// the grouping loop is exactly the same as in the combiner (merge lines while the key stays the same)
// the only difference is what we do when a key is finished:
//   combiner -> prints the merged line
//   reducer  -> saves the merged value (K, G, stations) or checks it for the busiest interval
// and at the very end the reducer computes the averages, top k and busiest interval and prints everything
//
// we use exactly 1 reducer, because top k (over all stations) and busiest interval (over all intervals)
// need to see every key. this is the same lesson as hw2: we can only pick top k / busiest after
// everything has been fully merged

#include <iostream>
#include <string>
#include <vector>
#include <algorithm>
#include <iomanip>
#include "mr_common.hpp"
using namespace std;

// one station after all its lines have been merged (one line of TOP_STATIONS)
struct StationResult {
    long long station_id;
    long long count;
    double temp_sum;
    double rain_sum;
};

// sorting rule for top k: more measurements first, and if the count is the same then smaller station id first
bool isBetterStation(const StationResult &a, const StationResult &b) {
    if (a.count != b.count) return a.count > b.count;
    return a.station_id < b.station_id;
}

// everything the reducer remembers while reading the input
long long K = 0;
bool sawK = false;              // true if the header line was in the input

double G[MAX_VALUES];           // the fully merged global stats
bool sawG = false;              // true if there was at least one measurement

vector<StationResult> stations; // all the stations, we pick the top k at the end

long long busiestInterval = 0;
long long busiestCount = 0;
bool sawInterval = false;

// called once for every key, when all the lines of that key have been merged
// because the input is sorted, the value we get here is the final value of this key
void saveKey(const string &key, const double total[]) {
    if (key == "K") {
        K = (long long)total[0];
        sawK = true;
    } else if (key == "G") {
        for (int i = 0; i < MAX_VALUES; i++) G[i] = total[i];
        sawG = true;
    } else if (key[0] == 'S') {
        // key looks like "S12", so the station id is everything after the S
        StationResult s;
        s.station_id = stoll(key.substr(1));
        s.count = (long long)total[0];
        s.temp_sum = total[1];
        s.rain_sum = total[2];
        stations.push_back(s);
    } else if (key[0] == 'I') {
        // key looks like "I5", so the interval id is everything after the I
        long long interval_id = stoll(key.substr(1));
        long long count = (long long)total[0];

        // this interval's count is final, so we can directly compare it with the best one so far
        // tie break: same count -> smaller interval id wins
        if (!sawInterval || count > busiestCount || (count == busiestCount && interval_id < busiestInterval)) {
            busiestInterval = interval_id;
            busiestCount = count;
            sawInterval = true;
        }
    }
}

// prints the final answer in exactly the same format as hw2's printResults()
void printResults() {
    // empty input file -> sequential prints nothing, so we also print nothing
    if (!sawK && !sawG) return;

    // header was there but N = 0 (no measurements) -> same as hw2
    if (!sawG) {
        cout << "TOTAL_MEASUREMENTS 0\n";
        return;
    }

    long long count = (long long)G[G_COUNT];

    cout << fixed << setprecision(6);

    cout << "TOTAL_MEASUREMENTS " << count << "\n";

    cout << "AVERAGE_TEMPERATURE " << G[G_TEMP_SUM] / count << "\n";
    cout << "MIN_TEMPERATURE " << G[G_TEMP_MIN] << "\n";
    cout << "MAX_TEMPERATURE " << G[G_TEMP_MAX] << "\n";

    cout << "AVERAGE_HUMIDITY " << G[G_HUM_SUM] / count << "\n";
    cout << "MIN_HUMIDITY " << G[G_HUM_MIN] << "\n";
    cout << "MAX_HUMIDITY " << G[G_HUM_MAX] << "\n";

    cout << "AVERAGE_PRESSURE " << G[G_PRES_SUM] / count << "\n";
    cout << "MIN_PRESSURE " << G[G_PRES_MIN] << "\n";
    cout << "MAX_PRESSURE " << G[G_PRES_MAX] << "\n";

    cout << "TOTAL_RAINFALL " << G[G_RAIN_SUM] << "\n";
    cout << "MAX_RAINFALL " << G[G_RAIN_MAX] << "\n";

    cout << "AVERAGE_WIND_SPEED " << G[G_WIND_SUM] / count << "\n";
    cout << "MAX_WIND_SPEED " << G[G_WIND_MAX] << "\n";

    cout << "EXTREME_TEMPERATURE_EVENTS " << (long long)G[G_EXTREME] << "\n";

    // station id and timestamp are whole numbers, so we cast them to long long (otherwise they print as 3.000000)
    cout << "HOTTEST_MEASUREMENT " << G[G_HOT_TEMP] << " "
         << (long long)G[G_HOT_STATION] << " " << (long long)G[G_HOT_TIME] << "\n";

    cout << "COLDEST_MEASUREMENT " << G[G_COLD_TEMP] << " "
         << (long long)G[G_COLD_STATION] << " " << (long long)G[G_COLD_TIME] << "\n";

    cout << "BUSIEST_INTERVAL " << busiestInterval << " " << busiestCount << "\n";

    // top k: sort all the stations best first, then print the first k
    sort(stations.begin(), stations.end(), isBetterStation);

    cout << "TOP_STATIONS\n";
    for (int i = 0; i < (int)stations.size() && i < K; i++) {
        const StationResult &s = stations[i];
        cout << s.station_id << " " << s.count << " " << s.temp_sum / s.count << " " << s.rain_sum << "\n";
    }
}

int main() {
    // makes the io fast cause we have to read a lot of lines as input
    ios_base::sync_with_stdio(false);
    cin.tie(nullptr);

    // same grouping loop as the combiner
    string currentKey;
    double total[MAX_VALUES];
    bool haveKey = false;

    string line;
    string key;
    double values[MAX_VALUES];
    int numValues;

    while (getline(cin, line)) {
        if (!parseLine(line, key, values, numValues)) continue;

        if (haveKey && key == currentKey) {
            // same key as before, so just merge it into the total
            mergeValues(key, total, values, numValues);
        } else {
            // a new key started, so first save the finished previous key
            if (haveKey) saveKey(currentKey, total);

            // and then start the new key with this line's values
            currentKey = key;
            for (int i = 0; i < numValues; i++) total[i] = values[i];
            haveKey = true;
        }
    }

    // the last key never sees a "key changed" moment, so we save it here
    if (haveKey) saveKey(currentKey, total);

    printResults();

    return 0;
}
