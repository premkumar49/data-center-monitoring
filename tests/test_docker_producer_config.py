"""
Unit tests for Docker environment variable fallback and configuration parsing.

Verifies that environment variables (KAFKA_BOOTSTRAP_SERVERS, KAFKA_TOPIC,
NUM_RACKS, SERVERS_PER_RACK, TELEMETRY_INTERVAL) set inside Docker containers
properly override CLI defaults without breaking local execution.
"""

import os
import pytest

from producer.kafka_producer import parse_args, KafkaTelemetryProducer


def test_env_var_bootstrap_server_fallback(monkeypatch):
    """Verify KAFKA_BOOTSTRAP_SERVERS env var sets default bootstrap server."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    args = parse_args([])
    assert args.bootstrap_server == "kafka:9092"


def test_env_var_topic_fallback(monkeypatch):
    """Verify KAFKA_TOPIC env var sets default topic."""
    monkeypatch.setenv("KAFKA_TOPIC", "custom_docker_topic")
    args = parse_args([])
    assert args.topic == "custom_docker_topic"


def test_env_var_topology_scaling(monkeypatch):
    """Verify NUM_RACKS and SERVERS_PER_RACK env vars set topology defaults."""
    monkeypatch.setenv("NUM_RACKS", "10")
    monkeypatch.setenv("SERVERS_PER_RACK", "5")
    args = parse_args([])
    assert args.racks == 10
    assert args.servers_per_rack == 5


def test_cli_args_override_env_vars(monkeypatch):
    """Verify explicit CLI arguments take priority over environment variables."""
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
    monkeypatch.setenv("KAFKA_TOPIC", "env_topic")
    args = parse_args(["--bootstrap-server", "override_host:9092", "--topic", "cli_topic"])
    assert args.bootstrap_server == "override_host:9092"
    assert args.topic == "cli_topic"


def test_local_default_without_env_vars(monkeypatch):
    """Verify local execution defaults to localhost:9092 when no env var is set."""
    monkeypatch.delenv("KAFKA_BOOTSTRAP_SERVERS", raising=False)
    monkeypatch.delenv("KAFKA_TOPIC", raising=False)
    args = parse_args([])
    assert args.bootstrap_server == "localhost:9092"
    assert args.topic == "server_telemetry"
