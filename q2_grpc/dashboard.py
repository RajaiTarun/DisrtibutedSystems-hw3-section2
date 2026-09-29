"""
CLI dashboard: asks the coordinator for the current analytics

usage: python dashboard.py <coordinator_address> [mode] [options]
    (no mode)          live view: refreshes the screen every --interval ms (default 500), Ctrl+C to stop
    --once             prints the current analytics once, in exactly the HW2 output format
    --bench            sends queries every --interval ms (default 0 = back to back) until the stream is
                       done, then prints the query latencies (used by the benchmark script). it also
                       checks that the processed count never goes down between two queries
    --stats            prints one summary line (records per worker, elapsed, rate), used by the benchmarks
    --shutdown         stops the coordinator and all its workers
options:
    --interval <ms>    time between two queries
    --exit-when-done   live view: stop after showing the final result
"""

import sys
import time
import argparse

import grpc
import weather_pb2
import weather_pb2_grpc
from grpc_common import make_channel


def query(stub):
    """one GetAnalytics call, returns (reply, latency in ms)"""
    t0 = time.time()
    reply = stub.GetAnalytics(weather_pb2.AnalyticsRequest())
    return reply, (time.time() - t0) * 1000


def print_live(address, r, latency_ms):
    if r.stream_done:
        state = "DONE"
    elif r.records_received > 0:
        state = "STREAMING"
    else:
        state = "WAITING FOR DATA"
    lines = [
        "\033[2J\033[H",   # clear the screen, cursor to the top
        "=============== Weather analytics dashboard ===============",
        f"coordinator       : {address}",
        f"status            : {state}",
        f"records processed : {r.records_processed:,}   (received {r.records_received:,})",
        f"elapsed           : {r.elapsed_s:.1f} s",
        f"ingest rate       : {r.ingest_rate:,.0f} records/s",
        f"workers           : {r.num_workers} (strategy {r.strategy})",
        "records per worker: " + " ".join(f"{c:,}" for c in r.records_per_worker),
        f"query latency     : {latency_ms:.2f} ms",
        "-----------------------------------------------------------",
        r.result if r.result else "(no analytics yet)\n",
    ]
    sys.stdout.write("\n".join(lines))
    sys.stdout.flush()


def bench(stub, interval_ms):
    latencies = []
    max_processed = 0
    monotonic = True   # every worker's count only grows, so the total must never go down
    while True:
        reply, latency = query(stub)
        # only measure once the stream has started: queries on an empty system are much cheaper
        if reply.records_received == 0 and not reply.stream_done:
            time.sleep(0.001)
            continue
        latencies.append(latency)
        if reply.records_processed < max_processed:
            monotonic = False
        max_processed = max(max_processed, reply.records_processed)
        if reply.stream_done:
            break
        if interval_ms > 0:
            time.sleep(interval_ms / 1000)
    latencies.sort()
    n = len(latencies)
    print(f"QUERIES count={n} mean_ms={sum(latencies) / n:.3f} p50_ms={latencies[n // 2]:.3f} "
          f"p95_ms={latencies[min(n - 1, int(n * 0.95))]:.3f} max_ms={latencies[-1]:.3f} "
          f"max_processed={max_processed} monotonic={'yes' if monotonic else 'no'}")


def main():
    parser = argparse.ArgumentParser(description="CLI dashboard for the weather analytics stream")
    parser.add_argument("address", help="coordinator address, e.g. node01:50051")
    parser.add_argument("--once", action="store_true", help="print the current analytics in the HW2 format")
    parser.add_argument("--bench", action="store_true", help="measure query latency until the stream is done")
    parser.add_argument("--stats", action="store_true", help="print one summary line")
    parser.add_argument("--shutdown", action="store_true", help="stop the coordinator and the workers")
    parser.add_argument("--interval", type=int, default=None, help="ms between two queries")
    parser.add_argument("--exit-when-done", action="store_true", help="live view: stop when the stream is done")
    args = parser.parse_args()

    stub = weather_pb2_grpc.CoordinatorStub(make_channel(args.address))
    try:
        if args.once:
            reply, _ = query(stub)
            sys.stdout.write(reply.result)
        elif args.shutdown:
            stub.Shutdown(weather_pb2.Empty())
        elif args.stats:
            r, _ = query(stub)
            per_worker = ";".join(str(c) for c in r.records_per_worker)
            print(f"STATS processed={r.records_processed} received={r.records_received} elapsed_s={r.elapsed_s:.4f} "
                  f"rate={r.ingest_rate:.1f} done={'yes' if r.stream_done else 'no'} per_worker={per_worker}")
        elif args.bench:
            bench(stub, args.interval if args.interval is not None else 0)
        else:
            interval = args.interval if args.interval is not None else 500
            while True:
                reply, latency = query(stub)
                print_live(args.address, reply, latency)
                if args.exit_when_done and reply.stream_done:
                    break
                time.sleep(interval / 1000)
    except grpc.RpcError as e:
        print(f"dashboard: {e.details()}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
