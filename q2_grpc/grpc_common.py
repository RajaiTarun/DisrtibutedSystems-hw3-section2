"""small helpers shared by the gRPC programs"""

import sys
import grpc

# gRPC's default maximum message size is 4 MB. round-robin partial results (all intervals of a worker)
# can be bigger, so we remove the limit (-1) on every channel and server
MESSAGE_OPTIONS = [
    ("grpc.max_send_message_length", -1),
    ("grpc.max_receive_message_length", -1),
]


def make_channel(address, timeout_s=60):
    """connects to 'host:port' and waits until the server is reachable (it may still be starting)"""
    channel = grpc.insecure_channel(address, options=MESSAGE_OPTIONS)
    try:
        grpc.channel_ready_future(channel).result(timeout=timeout_s)
    except grpc.FutureTimeoutError:
        print(f"could not connect to {address}", file=sys.stderr)
        sys.exit(1)
    return channel


def to_tuple(r):
    """weather_pb2.Record -> measurement tuple used by weather_stats.Stats"""
    return (r.timestamp, r.station_id, r.temperature, r.humidity, r.pressure, r.rainfall, r.wind_speed)
