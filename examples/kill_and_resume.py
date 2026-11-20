"""Run once with --kill to simulate interruption; run again without it to resume."""

import argparse
import os
import signal
import time

import ninelives as nl


def work(x: int) -> int:
    time.sleep(0.01)
    return x * x


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--kill", action="store_true")
    args = parser.parse_args()
    # Demonstration uses bounded work and process termination between calls.
    with nl.Checkpoint("self-kill-demo", fingerprint=nl.fingerprint((work, 100))) as ck:
        for i, _ in ck.pending(enumerate(range(100))):
            ck.save(i, work(i))
            if args.kill and i == 23:
                ck.commit()
                os.kill(os.getpid(), signal.SIGKILL)
    print("Completed; rerun without --kill to inspect/resume committed indices.")
