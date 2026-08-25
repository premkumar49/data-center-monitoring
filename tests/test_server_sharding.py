"""
Unit tests for server sharding and replica server assignment logic.

Verifies single-replica, two-replica, four-replica, and uneven sharding math.
Ensures zero duplicate server ownership and zero missing servers across all shards.
Does not require a live Kubernetes cluster for execution.
"""

import pytest

from producer.server_config import SimulationConfig, calculate_server_shard
from producer.simulator import DataCenterSimulator
from producer.kafka_producer import parse_args


def test_single_replica_sharding():
    """Verify single replica receives all 20 servers (SRV001..SRV020)."""
    start_idx, end_idx = calculate_server_shard(total_servers=20, shard_index=0, total_shards=1)
    assert start_idx == 1
    assert end_idx == 20

    sim = DataCenterSimulator(
        config=SimulationConfig(num_racks=5, servers_per_rack=4, server_start_index=start_idx, server_end_index=end_idx),
        seed=42
    )
    records = sim.generate_step()
    server_ids = [r.server_id for r in records]

    assert len(server_ids) == 20
    assert server_ids[0] == "SRV001"
    assert server_ids[-1] == "SRV020"


def test_two_replica_sharding_no_duplicates_no_missing():
    """
    Verify 2-replica mode assigns SRV001..SRV010 to Shard 0 and SRV011..SRV020 to Shard 1.
    Asserts zero overlap and complete coverage.
    """
    start0, end0 = calculate_server_shard(total_servers=20, shard_index=0, total_shards=2)
    start1, end1 = calculate_server_shard(total_servers=20, shard_index=1, total_shards=2)

    assert (start0, end0) == (1, 10)
    assert (start1, end1) == (11, 20)

    sim0 = DataCenterSimulator(config=SimulationConfig(server_start_index=start0, server_end_index=end0), seed=42)
    sim1 = DataCenterSimulator(config=SimulationConfig(server_start_index=start1, server_end_index=end1), seed=42)

    set0 = {r.server_id for r in sim0.generate_step()}
    set1 = {r.server_id for r in sim1.generate_step()}

    # Assert exact sizes
    assert len(set0) == 10
    assert len(set1) == 10

    # Assert zero intersection (no duplicate server ownership)
    intersection = set0.intersection(set1)
    assert len(intersection) == 0, f"Duplicate servers found between shards: {intersection}"

    # Assert complete union (no missing servers)
    union = set0.union(set1)
    expected_all = {f"SRV{i:03d}" for i in range(1, 21)}
    assert union == expected_all, f"Missing servers in 2-replica setup: {expected_all - union}"


def test_four_replica_sharding():
    """Verify 4-replica mode partitions 20 servers into 4 equal shards of 5 servers."""
    shards = [calculate_server_shard(total_servers=20, shard_index=i, total_shards=4) for i in range(4)]
    assert shards == [(1, 5), (6, 10), (11, 15), (16, 20)]

    all_sets = []
    for start, end in shards:
        sim = DataCenterSimulator(config=SimulationConfig(server_start_index=start, server_end_index=end), seed=42)
        srv_set = {r.server_id for r in sim.generate_step()}
        assert len(srv_set) == 5
        all_sets.append(srv_set)

    # Check pairwise disjointness
    for i in range(len(all_sets)):
        for j in range(i + 1, len(all_sets)):
            assert len(all_sets[i].intersection(all_sets[j])) == 0

    # Check union
    full_union = set().union(*all_sets)
    expected_all = {f"SRV{i:03d}" for i in range(1, 21)}
    assert full_union == expected_all


def test_uneven_server_sharding():
    """Verify uneven sharding (20 servers / 3 replicas -> 7, 7, 6 servers)."""
    s0 = calculate_server_shard(total_servers=20, shard_index=0, total_shards=3)
    s1 = calculate_server_shard(total_servers=20, shard_index=1, total_shards=3)
    s2 = calculate_server_shard(total_servers=20, shard_index=2, total_shards=3)

    assert s0 == (1, 7)
    assert s1 == (8, 14)
    assert s2 == (15, 20)

    set0 = {f"SRV{i:03d}" for i in range(s0[0], s0[1] + 1)}
    set1 = {f"SRV{i:03d}" for i in range(s1[0], s1[1] + 1)}
    set2 = {f"SRV{i:03d}" for i in range(s2[0], s2[1] + 1)}

    assert len(set0) == 7
    assert len(set1) == 7
    assert len(set2) == 6

    total_union = set0 | set1 | set2
    expected_all = {f"SRV{i:03d}" for i in range(1, 21)}
    assert total_union == expected_all
    assert len(set0 & set1) == 0
    assert len(set1 & set2) == 0
    assert len(set0 & set2) == 0


def test_sharding_cli_and_env_args(monkeypatch):
    """Verify CLI flags --shard-index and --total-shards compute start and end server indices."""
    args = parse_args(["--shard-index", "1", "--total-shards", "2", "--racks", "5", "--servers-per-rack", "4"])
    assert args.shard_index == 1
    assert args.total_shards == 2

    start_idx, end_idx = calculate_server_shard(args.racks * args.servers_per_rack, args.shard_index, args.total_shards)
    assert (start_idx, end_idx) == (11, 20)


def test_invalid_shard_parameters():
    """Verify invalid shard index or total shards raises ValueError."""
    with pytest.raises(ValueError):
        calculate_server_shard(total_servers=20, shard_index=-1, total_shards=2)

    with pytest.raises(ValueError):
        calculate_server_shard(total_servers=20, shard_index=2, total_shards=2)

    with pytest.raises(ValueError):
        calculate_server_shard(total_servers=20, shard_index=0, total_shards=0)
