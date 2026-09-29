"""
runs a command and writes its peak memory and cpu time to a file (works on mac and linux)

usage: python run_measured.py <output_file> <command> [args ...]
writes one line:  <name> max_rss_mb=... user_s=... sys_s=... wall_s=...
"""

import sys
import time
import resource
import subprocess


def main():
    out_file, command = sys.argv[1], sys.argv[2:]
    start = time.time()
    subprocess.call(command)
    wall = time.time() - start
    usage = resource.getrusage(resource.RUSAGE_CHILDREN)
    # ru_maxrss is in bytes on macOS and in kilobytes on linux
    rss_mb = usage.ru_maxrss / (1024 * 1024) if sys.platform == "darwin" else usage.ru_maxrss / 1024
    name = " ".join(a for a in command[1:2])   # the script name, e.g. worker.py
    with open(out_file, "w") as f:
        f.write(f"{name} max_rss_mb={rss_mb:.1f} user_s={usage.ru_utime:.2f} sys_s={usage.ru_stime:.2f} wall_s={wall:.2f}\n")


if __name__ == "__main__":
    main()
