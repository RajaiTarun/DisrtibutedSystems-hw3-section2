"""
streaming client: replays a dataset file as if the records were arriving live

usage: python client.py <coordinator_address> <input_file> [batch_size] [rate]
    batch_size : records per gRPC message (default 1000)
    rate       : records per second to send (default 0 = as fast as possible)

steps:
    1. read the file and build the RecordBatch messages in memory first, so that reading the disk
       and building the messages is not part of the measured time (like HW2, the first line is
       'N K S' and at most N records are read)
    2. Configure: send the header (N K S) to the coordinator
    3. StreamRecords: send the batches over ONE client stream, respecting the rate. gRPC takes the
       batches from a generator function; the generator sleeps when we are ahead of the rate
    4. StreamRecords only returns when the coordinator has processed every record, so the time until
       it returns is the end-to-end processing time

the last line of the output is machine readable (used by the benchmark script):
    RESULT records=... batch=... rate=... send_s=... total_s=... throughput=...
"""

import sys
import time

import grpc
import weather_pb2
import weather_pb2_grpc
from grpc_common import make_channel


def main():
    if len(sys.argv) < 3:
        print("usage: python client.py <coordinator_address> <input_file> [batch_size] [rate]", file=sys.stderr)
        sys.exit(1)
    address = sys.argv[1]
    input_file = sys.argv[2]
    batch_size = max(1, int(sys.argv[3])) if len(sys.argv) > 3 else 1000
    rate = float(sys.argv[4]) if len(sys.argv) > 4 else 0.0

    # ---------------- 1. read the file, build the batches ----------------
    # the Record messages are built directly while reading (no extra list of tuples), to save memory
    t0 = time.time()
    has_header, N, K, S = False, 0, 0, 0
    batches = []
    total_records = 0
    with open(input_file) as f:
        for line in f:
            parts = line.split()
            if len(parts) >= 3:
                N, K, S = int(parts[0]), int(parts[1]), int(parts[2])
                has_header = True
                break
        if has_header:
            batch = weather_pb2.RecordBatch()
            for line in f:
                parts = line.split()
                if len(parts) < 7 or total_records >= N:
                    continue
                batch.records.add(timestamp=int(parts[0]), station_id=int(parts[1]),
                                  temperature=float(parts[2]), humidity=float(parts[3]),
                                  pressure=float(parts[4]), rainfall=float(parts[5]), wind_speed=float(parts[6]))
                total_records += 1
                if len(batch.records) == batch_size:
                    batches.append(batch)
                    batch = weather_pb2.RecordBatch()
            if len(batch.records) > 0:
                batches.append(batch)
    print(f"client: read {total_records} records into {len(batches)} batches in {time.time() - t0:.2f} s",
          file=sys.stderr)

    # ---------------- 2. connect and configure ----------------
    stub = weather_pb2_grpc.CoordinatorStub(make_channel(address))
    try:
        stub.Configure(weather_pb2.StreamConfig(has_header=has_header, n=N, k=K, s=S))
    except grpc.RpcError as e:
        print(f"client: Configure failed: {e.details()}", file=sys.stderr)
        sys.exit(1)

    # ---------------- 3. stream ----------------
    times = {}

    def generate():
        """gives gRPC the batches one by one; waits when we are ahead of the requested rate"""
        start = time.time()
        times["start"] = start
        sent = 0
        for batch in batches:
            if rate > 0:
                # the first record of this batch should leave at start + sent / rate
                wait = start + sent / rate - time.time()
                if wait > 0:
                    time.sleep(wait)
            yield batch
            sent += len(batch.records)
        times["send_end"] = time.time()

    try:
        ack = stub.StreamRecords(generate())   # returns when everything is processed
    except grpc.RpcError as e:
        print(f"client: stream failed: {e.details()}", file=sys.stderr)
        sys.exit(1)
    end = time.time()

    # ---------------- 4. results ----------------
    start = times.get("start", end)
    send_s = times.get("send_end", end) - start
    total_s = end - start
    sent = total_records
    throughput = sent / total_s if total_s > 0 else 0.0
    print(f"client: sent {sent} records in {send_s:.3f} s, all processed after {total_s:.3f} s "
          f"({throughput:,.0f} records/s), coordinator says: {ack.message}", file=sys.stderr)
    print(f"RESULT records={sent} batch={batch_size} rate={rate:.0f} send_s={send_s:.4f} "
          f"total_s={total_s:.4f} throughput={throughput:.1f}")


if __name__ == "__main__":
    main()
