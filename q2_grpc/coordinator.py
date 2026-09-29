"""
coordinator: receives the stream from the client, sends the records to the workers, and answers
analytics queries by merging the workers' partial results

usage: python coordinator.py <listen_address> <strategy> <worker_address> [<worker_address> ...]
       e.g. python coordinator.py 0.0.0.0:50051 interval localhost:50061 localhost:50062

strategy = which worker gets a record:
    interval   -> worker = (timestamp // 60) % W   (every interval lives on exactly one worker)
    roundrobin -> record 0 to worker 0, record 1 to worker 1, ...   (perfectly even load)

how records travel:
    StreamRecords reads a batch from the client, splits it into one small batch per worker and puts
    those into the worker's queue. every worker has its own sender thread that takes batches from its
    queue and calls ProcessBatch on the worker, so all workers work at the same time.
    the queues have a maximum size: if a worker is slow, StreamRecords waits (back pressure), so the
    coordinator never fills its memory with unsent batches.
"""

import sys
import time
import queue
import threading
from concurrent import futures

import grpc
import weather_pb2
import weather_pb2_grpc
from weather_stats import Stats
from grpc_common import MESSAGE_OPTIONS, make_channel

MAX_QUEUE = 64   # batches waiting per worker before StreamRecords has to wait


class WorkerLink:
    """everything the coordinator needs for one worker"""

    def __init__(self, address):
        self.address = address
        self.stub = weather_pb2_grpc.WorkerStub(make_channel(address))
        self.queue = queue.Queue(maxsize=MAX_QUEUE)   # batches waiting to be sent
        self.records_done = 0                         # records this worker has processed
        self.lock = threading.Lock()                  # protects records_done
        self.thread = threading.Thread(target=self.sender_loop, daemon=True)
        self.thread.start()

    def sender_loop(self):
        """takes batches from the queue and sends them to the worker; None means stop"""
        while True:
            batch = self.queue.get()
            if batch is None:
                self.queue.task_done()
                return
            try:
                self.stub.ProcessBatch(batch)
                with self.lock:
                    self.records_done += len(batch.records)
            except grpc.RpcError as e:
                print(f"coordinator: ProcessBatch to {self.address} failed: {e.details()}", file=sys.stderr)
            self.queue.task_done()

    def wait_until_empty(self):
        """waits until every batch put in the queue has been sent and processed"""
        self.queue.join()


class CoordinatorServicer(weather_pb2_grpc.CoordinatorServicer):
    def __init__(self, workers, strategy, stop_event):
        self.workers = workers
        self.strategy = strategy
        self.stop_event = stop_event

        self.lock = threading.Lock()   # protects everything below
        self.configured = False
        self.has_header = False
        self.K = 0
        self.S = 0
        self.start_time = 0.0
        self.end_time = 0.0
        self.stream_done = False
        self.records_received = 0

    def Configure(self, request, context):
        with self.lock:
            self.has_header = request.has_header
            self.K = request.k
            self.S = request.s
            # every worker starts with empty stats
            config = weather_pb2.WorkerConfig(s=self.S, send_all_intervals=(self.strategy == "roundrobin"))
            for w in self.workers:
                try:
                    w.stub.Configure(config)
                except grpc.RpcError:
                    context.abort(grpc.StatusCode.UNAVAILABLE, f"could not configure worker {w.address}")
                with w.lock:
                    w.records_done = 0
            self.records_received = 0
            self.stream_done = False
            self.start_time = time.time()
            self.end_time = 0.0
            self.configured = True
        return weather_pb2.Ack(message="configured")

    def StreamRecords(self, request_iterator, context):
        if not self.configured:
            context.abort(grpc.StatusCode.FAILED_PRECONDITION, "call Configure first")
        W = len(self.workers)
        next_worker = 0   # for round-robin
        total = 0

        for batch in request_iterator:
            records = batch.records
            n = len(records)
            # split the batch into one list of records per worker
            parts = [[] for _ in range(W)]
            if self.strategy == "roundrobin":
                for w in range(W):
                    # records next_worker, next_worker + W, ... go to worker 0, and so on
                    parts[w] = records[(w - next_worker) % W::W]
                next_worker = (next_worker + n) % W
            else:
                for r in records:
                    parts[(r.timestamp // 60) % W].append(r)

            for w in range(W):
                if parts[w]:
                    # put() waits while the worker's queue is full (back pressure)
                    self.workers[w].queue.put(weather_pb2.RecordBatch(records=parts[w]))
            with self.lock:
                self.records_received += n
            total += n

        # the client has sent everything: wait until the workers have processed all of it
        for w in self.workers:
            w.wait_until_empty()
        with self.lock:
            self.end_time = time.time()
            self.stream_done = True
        return weather_pb2.Ack(records=total, message="all records processed")

    def GetAnalytics(self, request, context):
        with self.lock:
            K, S = self.K, self.S
            header = self.configured and self.has_header
            configured = self.configured
            start, end, done = self.start_time, self.end_time, self.stream_done
            received = self.records_received

        reply = weather_pb2.AnalyticsReply(num_workers=len(self.workers), strategy=self.strategy,
                                           records_received=received, stream_done=done)
        processed = 0
        if header:
            # ask all workers at the same time (futures), then merge their partial stats
            calls = [w.stub.GetPartial.future(weather_pb2.Empty()) for w in self.workers]
            merged = Stats(S)
            for w, call in zip(self.workers, calls):
                try:
                    partial = call.result()
                except grpc.RpcError:
                    context.abort(grpc.StatusCode.UNAVAILABLE, f"worker {w.address} did not answer")
                merged.merge(Stats.from_proto(partial, S))
                reply.records_per_worker.append(partial.count)
            reply.result = merged.format_results(K)
            processed = merged.count
        else:
            # not configured yet, or the input file was empty: HW2 prints nothing in that case
            reply.records_per_worker.extend([0] * len(self.workers))

        elapsed = ((end if done else time.time()) - start) if configured else 0.0
        reply.records_processed = processed
        reply.elapsed_s = elapsed
        reply.ingest_rate = processed / elapsed if elapsed > 0 else 0.0
        return reply

    def Shutdown(self, request, context):
        # stop the workers first, then ourselves
        for w in self.workers:
            try:
                w.stub.Shutdown(weather_pb2.Empty())
            except grpc.RpcError:
                pass
        self.stop_event.set()
        return weather_pb2.Ack(message="coordinator shutting down")


def main():
    if len(sys.argv) < 4:
        print("usage: python coordinator.py <listen_address> <interval|roundrobin> <worker_address> ...",
              file=sys.stderr)
        sys.exit(1)
    address, strategy, worker_addresses = sys.argv[1], sys.argv[2], sys.argv[3:]
    if strategy not in ("interval", "roundrobin"):
        print("strategy must be interval or roundrobin", file=sys.stderr)
        sys.exit(1)

    # connects to every worker (waits up to 60 s for each, they may still be starting)
    workers = [WorkerLink(a) for a in worker_addresses]

    stop_event = threading.Event()
    # enough threads for the client stream + many dashboards at the same time
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=64), options=MESSAGE_OPTIONS)
    weather_pb2_grpc.add_CoordinatorServicer_to_server(CoordinatorServicer(workers, strategy, stop_event), server)
    try:
        port = server.add_insecure_port(address)
    except RuntimeError:
        port = 0
    if port == 0:
        print(f"coordinator: could not listen on {address} (is the port already used by another program?)",
              file=sys.stderr)
        sys.exit(1)
    server.start()
    print(f"coordinator listening on {address} with {len(workers)} workers, strategy {strategy}", file=sys.stderr)

    stop_event.wait()
    server.stop(grace=1).wait()
    for w in workers:
        w.queue.put(None)   # stops the sender threads


if __name__ == "__main__":
    main()
