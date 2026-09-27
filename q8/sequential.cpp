#include "q8_common.hpp"
#include <iostream>
#include <chrono>
#include <cstdio>
using namespace std;

int main() {
    // io optimisation is needed when we are taking a lot of records in input
    ios_base::sync_with_stdio(false); // normally cin and cout are synced with c streams, but we disable that so less sync overhead and we have faster IO
    cin.tie(nullptr); // normally cin is tied to cout and so we are just breaking that and due to this everytime the cout doesn't get flushed

    int N, K, S;
    if(!(cin >> N >> K >> S)){
        return 0;
    }

    if (N == 0) {
        cout << "TOTAL_MEASUREMENTS 0\n";
        return 0;
    }

    auto t_read_start = chrono::steady_clock::now();
    vector<Measurement> data(N);
    for (int i = 0; i < N; i++) {
        cin >> data[i].timestamp >> data[i].station_id
            >> data[i].temperature >> data[i].humidity >> data[i].pressure
            >> data[i].rainfall >> data[i].wind_speed;
    }
    auto t_read_end = chrono::steady_clock::now();

    const int P = 9;  // 1 logical master + 8 logical workers
    auto t_compute_start = chrono::steady_clock::now();
    Stats globalStats = runMasterWorkerSimulation(data, N, S, P);
    auto t_compute_end = chrono::steady_clock::now();

    printResults(globalStats, K);

    // timing breakdown, stderr only - never touches the required stdout
    // output. mirrors mpi.cpp's read_input/compute split so the two
    // implementations' compute-only times can be compared like-for-like
    // instead of against sequential's whole run (which also includes its
    // own input read, same as MPI's read_input phase does).
    double read_time = chrono::duration<double>(t_read_end - t_read_start).count();
    double compute_time = chrono::duration<double>(t_compute_end - t_compute_start).count();
    fprintf(stderr, "[N=%d] read_input=%.3fs compute=%.3fs\n", N, read_time, compute_time);

    return 0;
}
