// notes:

// Sorted input:

// S0    1 20 1
// S0    1 30 2
// S0    1 25 3
// S1    1 40 5
// I0    1
// I0    1
// I1    1

// Combiner ko karna hai:

// S0 → merge all 3
// S1 → merge
// I0 → merge 2
// I1 → merge

// Output:

// S0    3 75 6
// S1    1 40 5
// I0    2
// I1    1



// combiner for q8 using mapreduce
// it runs on the output of one mapper, after that output has been sorted by key
// because it is sorted, all lines with the same key come one after another,
// so we just keep merging lines while the key stays the same, and when the key changes
// we print the merged line and start again with the new key
//
// the output format is exactly the same as the input format (key<TAB>values),
// so it does not matter if the combiner runs 0, 1 or many times, the final answer stays the same

#include <iostream>
#include <string>
#include <iomanip>
#include "mr_common.hpp"
using namespace std;

// prints one merged line in the same "key<TAB>v1 v2 ..." format that the mapper uses
void printLine(const string &key, const double total[], int numValues) {
    cout << key << "\t";
    for (int i = 0; i < numValues; i++) {
        if (i > 0) cout << " ";
        cout << total[i];
    }
    cout << "\n";
}

int main() {
    // makes the io fast cause we have to read a lot of lines as input
    ios_base::sync_with_stdio(false);
    cin.tie(nullptr);
    // 17 digits so that no precision is lost when the numbers go to the next stage
    cout << setprecision(17);

    string currentKey;          // basically this is the key jo ham abhi merge kar rahe hai
    double total[MAX_VALUES];   // merged values of the particular key
    int totalSize = 0;          // how many numbers are in the value of the current key
    bool haveKey = false;       // false until we have read the first line

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
            // a new key started, so first print the finished previous key
            if (haveKey) printLine(currentKey, total, totalSize);

            // and then start the new key with this line's values
            currentKey = key;
            totalSize = numValues;
            for (int i = 0; i < numValues; i++) total[i] = values[i];
            haveKey = true;
        }
    }

    // the last key never sees a "key changed" moment, so we print it here
    if (haveKey) printLine(currentKey, total, totalSize);

    return 0;
}
