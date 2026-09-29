"""
worker: keeps a Stats object (weather_stats.py) for the records the coordinator sends to it

usage: python worker.py <listen_address>          e.g. python worker.py 0.0.0.0:50061

rpcs (called by the coordinator):
    Configure    -> starts a new, empty Stats (number of stations S, which intervals to report)
    ProcessBatch -> Stats.update() for every record of the batch
    GetPartial   -> returns this worker's Stats as a PartialStats message
    Shutdown     -> stops the worker

gRPC runs every call in a thread of its thread pool, so ProcessBatch and GetPartial can arrive at the
same time. one lock protects the Stats, so a query always sees whole batches, never half of one.

every worker is its own process: python threads cannot run python code at the same time (the GIL),
but separate processes can, so W workers really work in parallel.
"""

import sys
import threading
from concurrent import futures

import grpc
import weather_pb2
import weather_pb2_grpc
from weather_stats import Stats
from grpc_common import MESSAGE_OPTIONS


class WorkerServicer(weather_pb2_grpc.WorkerServicer):
    def __init__(self, stop_event):
        self.lock = threading.Lock()   # protects everything below
        self.stats = None              # None until Configure is called
        self.S = 0
        self.send_all_intervals = False
        self.stop_event = stop_event

    def Configure(self, request, context):
        with self.lock:
            self.S = request.s
            self.send_all_intervals = request.send_all_intervals
            self.stats = Stats(self.S)   # fresh, empty stats for the new stream
        return weather_pb2.Ack(message="configured")

    def ProcessBatch(self, request, context):
        with self.lock:
            if self.stats is None:
                context.abort(grpc.StatusCode.FAILED_PRECONDITION, "worker is not configured")
            update = self.stats.update
            S = self.S
            for r in request.records:
                if 0 <= r.station_id < S:   # an invalid station id would crash the station lists
                    update((r.timestamp, r.station_id, r.temperature, r.humidity,
                            r.pressure, r.rainfall, r.wind_speed))
        return weather_pb2.Ack(records=len(request.records))

    def GetPartial(self, request, context):
        with self.lock:
            if self.stats is None:
                return weather_pb2.PartialStats(has_measurement=False)
            return self.stats.to_proto(self.send_all_intervals)

    def Shutdown(self, request, context):
        self.stop_event.set()   # main() sees this and stops the server
        return weather_pb2.Ack(message="worker shutting down")


def main():
    if len(sys.argv) < 2:
        print("usage: python worker.py <listen_address>   e.g. python worker.py 0.0.0.0:50061", file=sys.stderr)
        sys.exit(1)
    address = sys.argv[1]

    stop_event = threading.Event()
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=8), options=MESSAGE_OPTIONS)
    weather_pb2_grpc.add_WorkerServicer_to_server(WorkerServicer(stop_event), server)
    if server.add_insecure_port(address) == 0:
        print(f"worker: could not listen on {address}", file=sys.stderr)
        sys.exit(1)
    server.start()
    print(f"worker listening on {address}", file=sys.stderr)

    stop_event.wait()
    server.stop(grace=1).wait()


if __name__ == "__main__":
    main()
