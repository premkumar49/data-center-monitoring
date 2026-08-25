"""
CLI Entry point for Data Center Telemetry Simulator.

Supports:
  - Continuous mode (real-time stream simulation with configurable interval)
  - Test mode (finite records generated instantly for fast integration/unit testing)
  - Scenario mode (deterministic incident injection)
  - Topology scaling (--racks, --servers-per-rack)
  - Reproducibility (--seed)
"""

import argparse
import os
import sys
import time
from typing import List

# Ensure parent directory is in sys.path for direct script execution
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

from producer.server_config import SimulationConfig
from producer.simulator import DataCenterSimulator


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Data Center Telemetry Simulator CLI"
    )
    parser.add_argument(
        "--test",
        action="store_true",
        help="Run in finite test mode without sleep delays.",
    )
    parser.add_argument(
        "--records",
        type=int,
        default=20,
        help="Number of records to generate in test mode (default: 20).",
    )
    parser.add_argument(
        "--interval",
        type=float,
        default=5.0,
        help="Telemetry interval in seconds for continuous mode (default: 5.0).",
    )
    parser.add_argument(
        "--racks",
        type=int,
        default=5,
        help="Number of racks in simulation topology (default: 5).",
    )
    parser.add_argument(
        "--servers-per-rack",
        type=int,
        default=4,
        help="Number of servers per rack (default: 4).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=None,
        help="Optional random seed for reproducible output.",
    )
    parser.add_argument(
        "--disable-incidents",
        action="store_true",
        help="Disable automatic global incident generation.",
    )
    parser.add_argument(
        "--scenario",
        type=str,
        choices=[
            "normal",
            "cpu_overload",
            "overheating",
            "disk_saturation",
            "network_congestion",
            "memory_pressure",
        ],
        default=None,
        help="Inject a specific incident scenario deterministically.",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    config = SimulationConfig(
        num_racks=args.racks,
        servers_per_rack=args.servers_per_rack,
        telemetry_interval=args.interval,
        enable_incidents=not args.disable_incidents,
    )

    simulator = DataCenterSimulator(
        config=config, seed=args.seed, scenario=args.scenario
    )

    if args.test:
        # Test Mode: Generate finite number of records immediately
        records_emitted = 0
        while records_emitted < args.records:
            step_records = simulator.generate_step()
            for record in step_records:
                print(record.to_json())
                sys.stdout.flush()
                records_emitted += 1
                if records_emitted >= args.records:
                    break
    else:
        # Continuous Mode: Stream telemetry every interval seconds
        try:
            while True:
                step_records = simulator.generate_step()
                for record in step_records:
                    print(record.to_json())
                    sys.stdout.flush()
                time.sleep(config.telemetry_interval)
        except KeyboardInterrupt:
            sys.exit(0)


if __name__ == "__main__":
    main()
