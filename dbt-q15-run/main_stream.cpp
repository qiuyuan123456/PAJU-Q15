#include "iprogram.hpp"
#include "q15.cpp"
#include <iostream>
#include <chrono>

using namespace std;
using namespace dbtoaster;

int main(int argc, char** argv)
{
    auto t_start = chrono::high_resolution_clock::now();

    cout << "[INFO] Q15 Benchmark Start" << endl;

    Q15 program(argc, argv);

    // -------------------------------
    // Measure init() time
    // -------------------------------
    auto t_init0 = chrono::high_resolution_clock::now();
    program.init();
    auto t_init1 = chrono::high_resolution_clock::now();

    // -------------------------------
    // Measure process_streams() time
    // -------------------------------
    auto t_proc0 = chrono::high_resolution_clock::now();
    program.process_streams();
    auto t_proc1 = chrono::high_resolution_clock::now();

    // -------------------------------
    // Measure snapshot extraction time
    // -------------------------------
    auto t_snap0 = chrono::high_resolution_clock::now();
    Program::snapshot_t snap = program.take_snapshot();
    auto t_snap1 = chrono::high_resolution_clock::now();

    // -------------------------------
    // Extract results
    // -------------------------------
    tlq_t* tlq = snap.get();
    const COUNT_map& result = tlq->get_COUNT();

    cout << "\n=== QUERY RESULT ===" << endl;

    int rows = 0;
    auto* e = result.head;

    while (e) {
        rows++;
        cout << "SUPPKEY = " << e->S_SUPPKEY
             << " | NAME = " << e->S_NAME
             << " | ADDRESS = " << e->S_ADDRESS
             << " | PHONE = " << e->S_PHONE
             << " | REVENUE = " << e->R1_TOTAL_REVENUE
             << endl;
        e = e->nxt;
    }

    cout << "Total rows = " << rows << endl;
    cout << "====================\n" << endl;

    // -------------------------------
    // Print timing statistics
    // -------------------------------
    double t_init_ms = chrono::duration<double, milli>(t_init1 - t_init0).count();
    double t_proc_ms = chrono::duration<double, milli>(t_proc1 - t_proc0).count();
    double t_snap_ms = chrono::duration<double, milli>(t_snap1 - t_snap0).count();

    double t_total_ms = chrono::duration<double, milli>(t_snap1 - t_start).count();

    cout << "=== PERFORMANCE ===" << endl;
    cout << "Init time            : " << t_init_ms  << " ms" << endl;
    cout << "Stream processing    : " << t_proc_ms  << " ms" << endl;
    cout << "Snapshot extraction  : " << t_snap_ms  << " ms" << endl;
    cout << "Total time (Q15 run) : " << t_total_ms << " ms" << endl;
    cout << "====================" << endl;

    // 用于画论文图表：输出简洁 benchmark 数据
    cout << "\nBENCHMARK_OUTPUT Q15 "
         << "TOTAL_MS=" << t_total_ms
         << endl;

    return 0;
}
