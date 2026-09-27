// basically this file has some common implementation that we will need in both the sequential implementation and the distributed implementation

#pragma once

#include <vector>
#include <unordered_map>
#include <utility>
#include <algorithm>

using namespace std;

// timestamp station_id temperature humidity pressure rainfall wind_speed
// this is how a measurement log is defined in the assignment, so making a struct for that

// field order matters for MPI: station_id (4 bytes) is placed last, after
// all the 8-byte fields, so there's no internal padding gap between members
// (only trailing padding after station_id, to align the struct to 8 bytes).
// that keeps the derived MPI datatype in mpi.cpp's createMeasurementType()
// contiguous, so MPI can pack/unpack it with a straight memcpy instead of
// per-field copies during MPI_Scatterv - this matters a lot at millions of
// records. all field access elsewhere is by name (never positional
// aggregate init), so this reordering doesn't affect anything else.
struct Measurement {
    long long timestamp;
    double temperature;
    double humidity;
    double pressure;
    double rainfall;
    double wind_speed;
    int station_id;
};

// for each station we will need the count of the measurements, temp_sum and rainfall_sum later to find the avg
struct StationStats {
    long long count = 0;
    double temp_sum = 0;
    double rainfall_sum = 0;
};

// one line of the TOP_STATIONS output
struct TopStationEntry {
    int station_id;
    long long count;
    double avg_temperature;
    double total_rainfall;
};

// the 4 averages that are needed for the output
struct FinalAverages {
    double avg_temperature;
    double avg_humidity;
    double avg_pressure;
    double avg_wind_speed;
};

// the BUSIEST_INTERVAL output line: interval_id = timestamp / 60, count = measurements in it
struct BusiestInterval {
    long long interval_id;
    long long count;
};

struct Stats {
    // basically each worker process will maintain stats structure for its process
    long long count = 0;
    double temp_sum = 0;
    double temp_min = 0;
    double temp_max = 0;
    double humidity_sum = 0;
    double humidity_min = 0;
    double humidity_max = 0;
    double pressure_sum = 0;
    double pressure_min = 0;
    double pressure_max = 0;
    double rainfall_sum = 0;
    double rainfall_max = 0;
    double wind_sum = 0;
    double wind_max = 0;
    long long extreme_count = 0;
    Measurement hottest;
    Measurement coldest;
    bool has_measurement = false;
    vector<StationStats> stations;
    unordered_map<long long, long long> intervals;

    Stats(int S) {
        stations.resize(S);
    }
};

// helper functions

// this function basically takes a measurement log and then updates the stats of the worker process accordingly
void updateStats(Stats &stats, const Measurement &m);

// just a helper function that will handle returning the hottest temp following the tie break rules
bool isHotter(const Measurement &a, const Measurement &b);

// just a helper function that will handle returning the coldest temp following the tie break rules
bool isColder(const Measurement &a, const Measurement &b);

// compares stations based on their measurement counts
bool isBetterStation(int a_id, const StationStats &a, int b_id, const StationStats &b);

// basically once we are done with each process doing its work now we need to merge them inorder to get final answer
void mergingWorkerProcessStats(Stats &dest, const Stats &source);

// this is nothing but workerprocess code
Stats computeRangeOnWorker(const vector<Measurement> &data, int begin, int end, int S);

// this function will gives us the data start and end range on which our worker process should work
pair<int, int> getPartition(int worker, int P, int N);

// sort of just simualating on the sequential as if it is distributed, but its just a single process on the local machine, just as we did for q2
Stats runMasterWorkerSimulation(const vector<Measurement> &data, int N, int S, int P);

// this is the helper function to get that top k stations, and the apporach that we used is that we will maintain a k size min heap
// initially what mistake i had done was that i was maintaining k size min heap for each process but that was resulting in actual measurement data loss and was not giving correct ans
// for example lets say we want top 2 and the data is 
// process A : station 1 : 501 station 2 : 501 station 3 : 500
// process B : station 4 : 501 station 5 : 501 station 3 : 500
// so my answer was coming out as : station 1 and 2 but the actual answer whould be 3 and 1
vector<TopStationEntry> getTopStations(const Stats &globalStats, int K);

// helper function to get the final averages for output
FinalAverages computeFinalAverages(const Stats &globalStats);

// hlper function that helps us get the busiest interval
BusiestInterval getBusiestInterval(const Stats &globalStats);

// this function basically prints the final output
void printResults(const Stats &globalStats, int K);

// this helper function basically runs the entire program without master worker simulation
Stats computeDirectBaseline(const vector<Measurement> &data, int N, int S);