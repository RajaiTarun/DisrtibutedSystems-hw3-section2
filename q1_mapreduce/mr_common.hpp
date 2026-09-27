// common code that both the combiner and the reducer need
// 1. reading one "key<TAB>values" line
// 2. merging two values that have the same key
//
// to keep things simple every value is stored as an array of doubles
// (counts, station ids and timestamps are whole numbers, but a double can hold
// whole numbers exactly up to 2^53, which is way bigger than anything we have)
//
// note: this header is included by only one .cpp file in each program
// (combiner.cpp or reducer.cpp), so it is fine to write the functions directly in here

#pragma once

#include <iostream>
#include <sstream>
#include <string>
using namespace std;

// the G value has 21 numbers, this is the biggest value we have
const int MAX_VALUES = 21;

// positions of each number inside the G value (same order as the mapper prints them)
const int G_COUNT = 0;
const int G_TEMP_SUM = 1, G_TEMP_MIN = 2, G_TEMP_MAX = 3;
const int G_HUM_SUM = 4, G_HUM_MIN = 5, G_HUM_MAX = 6;
const int G_PRES_SUM = 7, G_PRES_MIN = 8, G_PRES_MAX = 9;
const int G_RAIN_SUM = 10, G_RAIN_MAX = 11;
const int G_WIND_SUM = 12, G_WIND_MAX = 13;
const int G_EXTREME = 14;
const int G_HOT_TEMP = 15, G_HOT_STATION = 16, G_HOT_TIME = 17;
const int G_COLD_TEMP = 18, G_COLD_STATION = 19, G_COLD_TIME = 20;

// splits "key<TAB>v1 v2 v3 ..." into the key and the array of values
// returns false if the line has no tab (so its not a valid line and we skip it)
bool parseLine(const string &line, string &key, double values[], int &numValues) {
    size_t tabPos = line.find('\t');
    // basically we are just finding the position of tab in our line, cause tab separates the key and value
    if (tabPos == string::npos) return false;

    key = line.substr(0, tabPos); // separating the key

    // now filling the values array for this particular line
    stringstream ss(line.substr(tabPos + 1));
    numValues = 0;
    while (numValues < MAX_VALUES && ss >> values[numValues]) {
        numValues++;
    }
    return true;
}

// same tie break rules as hw2: higher temp wins, then smaller timestamp, then smaller station id
bool isHotter(double tempA, double timeA, double stationA, double tempB, double timeB, double stationB) {
    if (tempA != tempB) return tempA > tempB;
    if (timeA != timeB) return timeA < timeB;
    return stationA < stationB;
}

// same tie break rules as hw2: lower temp wins, then smaller timestamp, then smaller station id
bool isColder(double tempA, double timeA, double stationA, double tempB, double timeB, double stationB) {
    if (tempA != tempB) return tempA < tempB;
    if (timeA != timeB) return timeA < timeB;
    return stationA < stationB;
}

// merges the G value "v" into "total"
// this is the same idea as mergingWorkerProcessStats() from hw2
void mergeG(double total[], const double v[]) {
    // merging the line's data with the total data
    total[G_COUNT] += v[G_COUNT];

    total[G_TEMP_SUM] += v[G_TEMP_SUM];
    if (v[G_TEMP_MIN] < total[G_TEMP_MIN]) total[G_TEMP_MIN] = v[G_TEMP_MIN];
    if (v[G_TEMP_MAX] > total[G_TEMP_MAX]) total[G_TEMP_MAX] = v[G_TEMP_MAX];

    total[G_HUM_SUM] += v[G_HUM_SUM];
    if (v[G_HUM_MIN] < total[G_HUM_MIN]) total[G_HUM_MIN] = v[G_HUM_MIN];
    if (v[G_HUM_MAX] > total[G_HUM_MAX]) total[G_HUM_MAX] = v[G_HUM_MAX];

    total[G_PRES_SUM] += v[G_PRES_SUM];
    if (v[G_PRES_MIN] < total[G_PRES_MIN]) total[G_PRES_MIN] = v[G_PRES_MIN];
    if (v[G_PRES_MAX] > total[G_PRES_MAX]) total[G_PRES_MAX] = v[G_PRES_MAX];

    total[G_RAIN_SUM] += v[G_RAIN_SUM];
    if (v[G_RAIN_MAX] > total[G_RAIN_MAX]) total[G_RAIN_MAX] = v[G_RAIN_MAX];

    total[G_WIND_SUM] += v[G_WIND_SUM];
    if (v[G_WIND_MAX] > total[G_WIND_MAX]) total[G_WIND_MAX] = v[G_WIND_MAX];

    total[G_EXTREME] += v[G_EXTREME];

    // hottest: if v's hottest beats total's hottest, copy all 3 numbers (temp, station, timestamp)
    if (isHotter(v[G_HOT_TEMP], v[G_HOT_TIME], v[G_HOT_STATION],
                 total[G_HOT_TEMP], total[G_HOT_TIME], total[G_HOT_STATION])) {
        total[G_HOT_TEMP] = v[G_HOT_TEMP];
        total[G_HOT_STATION] = v[G_HOT_STATION];
        total[G_HOT_TIME] = v[G_HOT_TIME];
    }

    // coldest: same thing
    if (isColder(v[G_COLD_TEMP], v[G_COLD_TIME], v[G_COLD_STATION],
                 total[G_COLD_TEMP], total[G_COLD_TIME], total[G_COLD_STATION])) {
        total[G_COLD_TEMP] = v[G_COLD_TEMP];
        total[G_COLD_STATION] = v[G_COLD_STATION];
        total[G_COLD_TIME] = v[G_COLD_TIME];
    }
}

// merges the value "v" into "total" depending on what kind of key it is
void mergeValues(const string &key, double total[], const double v[], int numValues) {
    if (key == "K") {
        // all K lines have the same value, so nothing to merge
        return;
    }
    if (key == "G") {
        mergeG(total, v);
        return;
    }
    // S<station> (count temp_sum rain_sum) and I<interval> (count):
    // everything is a count or a sum, so we just add number by number
    for (int i = 0; i < numValues; i++) {
        total[i] += v[i];
    }
}
