"""small helpers shared by the gRPC programs"""

import os
import sys
import grpc

# gRPC's default maximum message size is 4 MB. round-robin partial results (all intervals of a worker)
# can be bigger, so we remove the limit (-1) on every channel and server
MESSAGE_OPTIONS = [
    ("grpc.max_send_message_length", -1),
    ("grpc.max_receive_message_length", -1),
]

# the cluster sets http_proxy / https_proxy (so that pip can reach the internet), and python gRPC
# would send its connections through that proxy too, even to 127.0.0.1 or other compute nodes.
# they never arrive and the client waits forever. all our connections are inside the cluster,
# so channels never use a proxy
CHANNEL_OPTIONS = MESSAGE_OPTIONS + [("grpc.enable_http_proxy", 0)]


def make_channel(address, timeout_s=None):
    """connects to 'host:port' and waits until the server is reachable (it may still be starting).
    waits at most 60 s, or GRPC_CONNECT_TIMEOUT seconds if that environment variable is set"""
    if timeout_s is None:
        timeout_s = float(os.environ.get("GRPC_CONNECT_TIMEOUT", "60"))
    channel = grpc.insecure_channel(address, options=CHANNEL_OPTIONS)
    try:
        grpc.channel_ready_future(channel).result(timeout=timeout_s)
    except grpc.FutureTimeoutError:
        print(f"could not connect to {address}", file=sys.stderr)
        sys.exit(1)
    return channel


def to_tuple(r):
    """weather_pb2.Record -> measurement tuple used by weather_stats.Stats"""
    return (r.timestamp, r.station_id, r.temperature, r.humidity, r.pressure, r.rainfall, r.wind_speed)
