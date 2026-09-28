// mapper for q8 using mapreduce
// it reads the input line by line from stdin and for every measurement it prints some key value pairs to stdout
// key and value are separated by a tab, because that is what hadoop streaming (and our sort) expects
//
// the keys that we print are:
//   K            -> the K value from the header line (reducer needs it for top k)
//   G            -> global partial stats (count, sums, mins, maxes, extreme count, hottest, coldest)
//   S<station>   -> count temp_sum rain_sum for that station
//   I<interval>  -> count for that 60 second interval
//
// for G, a single measurement is just a partial stats with count = 1, where
// sum = min = max = the value itself and hottest = coldest = this measurement
// this way the combiner and reducer only need to know how to merge two partial stats

#include <iostream>
#include <sstream>
#include <string>
#include <iomanip>
using namespace std;

int main() {
    // makes the io fast cause we have to read a lot of lines as input
    ios_base::sync_with_stdio(false);
    cin.tie(nullptr);

    string line;
    while (getline(cin, line)) {
        // first we read all the numbers of this line into an array, so we can know how many values it has
        stringstream ss(line);
        string tokens[8];
        int numTokens = 0;
        string word;
        while (numTokens < 8 && ss >> word) {
            tokens[numTokens] = word;
            numTokens++;
        }

        // header line "N K S" has 3 values, we only need K from it
        // (when the input is split into chunks, only the first chunk has the header, and thats fine)
        if (numTokens == 3) {
            cout << "K\t" << tokens[1] << "\n";
            continue;
        }

        // anything that is not a measurement line (like an empty line) is skipped
        if (numTokens != 7) continue;

        // timestamp station_id temperature humidity pressure rainfall wind_speed
        // we only convert the values that we need to calculate something with
        // all the other values are printed exactly as the text we read (see below)
        long long timestamp = stoll(tokens[0]);
        int station_id = stoi(tokens[1]);
        double temperature = stod(tokens[2]);

        // the measurement values are printed as the original text from the input, not as doubles
        // eg "24.49" stays "24.49", but if we printed the double with 17 digits it would become
        // "24.489999999999998", which makes the mapper output more than 2x bigger for no reason
        // (the combiner and reducer read the text back into the exact same double anyway)
        string temp = tokens[2];
        string hum = tokens[3];
        string pres = tokens[4];
        string rain = tokens[5];
        string wind = tokens[6];

        int extreme = 0;
        if (temperature >= 40.0 || temperature <= 0.0) extreme = 1;

        // G value format:
        // count
        // temp_sum temp_min temp_max
        // hum_sum hum_min hum_max
        // pres_sum pres_min pres_max
        // rain_sum rain_max
        // wind_sum wind_max
        // extreme_count
        // hottest_temp hottest_station hottest_timestamp
        // coldest_temp coldest_station coldest_timestamp
        cout << "G\t" << 1 << " "
             << temp << " " << temp << " " << temp << " "
             << hum << " " << hum << " " << hum << " "
             << pres << " " << pres << " " << pres << " "
             << rain << " " << rain << " "
             << wind << " " << wind << " "
             << extreme << " "
             << temp << " " << station_id << " " << timestamp << " "
             << temp << " " << station_id << " " << timestamp << "\n";

        // per station: count temp_sum rain_sum
        cout << "S" << station_id << "\t" << 1 << " " << temp << " " << rain << "\n";

        // per interval: count
        cout << "I" << (timestamp / 60) << "\t" << 1 << "\n";
    }

    return 0;
}
