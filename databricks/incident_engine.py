"""
Stateful Alert & Incident Detection Engine.

Consumes validated infrastructure telemetry events and maintains independent state machines
per (server_id, incident_type) key. Implements hysteresis recovery, deduplication,
temporal confirmation rules, unique incident IDs, and notification eligibility tagging.
"""

from datetime import datetime, timezone
from typing import Dict, List, Tuple, Optional, Any

from databricks.alert_rules import (
    AlertEngineConfig,
    AlertThreshold,
    EventType,
    IncidentStatus,
    IncidentType,
    Severity,
    format_alert_message,
)


class ServerIncidentState:
    """Internal state tracking for a specific (server_id, incident_type) pair."""

    def __init__(self, server_id: str, incident_type: IncidentType):
        self.server_id = server_id
        self.incident_type = incident_type

        self.incident_id: Optional[str] = None
        self.phase: str = "NORMAL"  # "NORMAL", "WARNING", "CRITICAL", "RECOVERY"
        self.severity: Optional[Severity] = None
        self.status: IncidentStatus = IncidentStatus.CLOSED

        self.consecutive_warning: int = 0
        self.consecutive_critical: int = 0
        self.consecutive_recovery: int = 0

        self.first_seen: Optional[str] = None
        self.last_seen: Optional[str] = None
        self.current_value: float = 0.0
        self.threshold_value: float = 0.0

        self.has_notified_critical: bool = False
        self.sequence_counter: int = 0


class StatefulIncidentEngine:
    """
    Stateful engine evaluating raw telemetry records against physical thresholds.
    
    Exclusively owns incident classification, deduplication, and notification tagging.
    Never modifies raw telemetry streams.
    """

    def __init__(self, config: Optional[AlertEngineConfig] = None):
        self.config = config or AlertEngineConfig()
        # Storage dictionary keyed by tuple: (server_id, incident_type)
        self.states: Dict[Tuple[str, IncidentType], ServerIncidentState] = {}
        self._global_sequence: int = 0

    def _get_state(self, server_id: str, incident_type: IncidentType) -> ServerIncidentState:
        key = (server_id, incident_type)
        if key not in self.states:
            self.states[key] = ServerIncidentState(server_id, incident_type)
        return self.states[key]

    def _extract_metric_value(self, record: dict, incident_type: IncidentType) -> float:
        threshold_info = self.config.thresholds[incident_type]
        if threshold_info.metric_field == "network_max":
            net_in = float(record.get("network_in", 0.0))
            net_out = float(record.get("network_out", 0.0))
            return max(net_in, net_out)
        return float(record.get(threshold_info.metric_field, 0.0))

    def _generate_incident_id(self, timestamp_str: str, server_id: str, incident_type: IncidentType) -> str:
        self._global_sequence += 1
        clean_ts = timestamp_str.replace("-", "").replace(":", "").replace("T", "").replace("Z", "")
        date_prefix = clean_ts[:8] if len(clean_ts) >= 8 else "20260825"
        
        short_code_map = {
            IncidentType.CPU_OVERLOAD: "CPU",
            IncidentType.OVERHEATING: "TEMP",
            IncidentType.DISK_SATURATION: "DISK",
            IncidentType.NETWORK_CONGESTION: "NET",
            IncidentType.MEMORY_PRESSURE: "MEM",
        }
        code = short_code_map.get(incident_type, "INC")
        return f"INC-{date_prefix}-{server_id}-{code}-{self._global_sequence:04d}"

    def process_record(self, record: dict) -> List[dict]:
        """
        Process a single validated telemetry record across all alert rules.
        
        Returns:
            List of derived incident event dictionaries (empty if no state transition occurs).
        """
        server_id = record["server_id"]
        rack_id = record["rack_id"]
        timestamp = record["timestamp"]
        generated_events: List[dict] = []

        for inc_type, threshold_rule in self.config.thresholds.items():
            val = self._extract_metric_value(record, inc_type)
            state = self._get_state(server_id, inc_type)

            event = self._evaluate_incident_state(
                state=state,
                record=record,
                val=val,
                threshold_rule=threshold_rule,
                timestamp=timestamp,
                rack_id=rack_id,
            )
            if event:
                generated_events.append(event)

        return generated_events

    def _evaluate_incident_state(
        self,
        state: ServerIncidentState,
        record: dict,
        val: float,
        threshold_rule: AlertThreshold,
        timestamp: str,
        rack_id: str,
    ) -> Optional[dict]:
        state.last_seen = timestamp
        state.current_value = val

        is_critical = val >= threshold_rule.critical_threshold
        is_warning = (val >= threshold_rule.warning_threshold) and not is_critical
        is_recovery = val < threshold_rule.recovery_threshold

        # Update counter tracking
        if is_critical:
            state.consecutive_critical += 1
            state.consecutive_warning += 1
            state.consecutive_recovery = 0
        elif is_warning:
            state.consecutive_warning += 1
            state.consecutive_critical = 0
            state.consecutive_recovery = 0
        elif is_recovery:
            state.consecutive_recovery += 1
            state.consecutive_warning = 0
            state.consecutive_critical = 0
        else:
            # Neutral band between recovery_threshold and warning_threshold
            state.consecutive_warning = 0
            state.consecutive_critical = 0

        event_to_emit: Optional[dict] = None

        # -------------------------------------------------------------
        # STATE MACHINE TRANSITIONS
        # -------------------------------------------------------------

        # 1. Transition to CRITICAL (from NORMAL, WARNING, or RECOVERY)
        if state.consecutive_critical >= self.config.critical_confirmation_count:
            if state.phase != "CRITICAL":
                old_phase = state.phase
                state.phase = "CRITICAL"
                state.severity = Severity.CRITICAL
                state.status = IncidentStatus.OPEN
                state.threshold_value = threshold_rule.critical_threshold

                if old_phase == "NORMAL" or not state.incident_id:
                    state.incident_id = self._generate_incident_id(timestamp, state.server_id, state.incident_type)
                    state.first_seen = timestamp
                    event_type = EventType.INCIDENT_OPENED
                else:
                    event_type = EventType.INCIDENT_ESCALATED

                notification_req = not state.has_notified_critical
                state.has_notified_critical = True

                msg = format_alert_message(
                    severity=Severity.CRITICAL,
                    incident_type=state.incident_type,
                    server_id=state.server_id,
                    rack_id=rack_id,
                    current_val=val,
                    threshold_val=threshold_rule.critical_threshold,
                    unit=threshold_rule.unit,
                    status=IncidentStatus.OPEN,
                )

                event_to_emit = self._build_event(
                    state=state,
                    rack_id=rack_id,
                    event_type=event_type,
                    notification_required=notification_req,
                    message=msg,
                )

        # 2. Transition to WARNING (from NORMAL or RECOVERY)
        elif is_warning and state.consecutive_warning >= self.config.warning_confirmation_count:
            if state.phase == "NORMAL" or state.phase == "RECOVERY":
                state.phase = "WARNING"
                state.severity = Severity.WARNING
                state.status = IncidentStatus.OPEN
                state.threshold_value = threshold_rule.warning_threshold
                state.incident_id = self._generate_incident_id(timestamp, state.server_id, state.incident_type)
                state.first_seen = timestamp

                msg = format_alert_message(
                    severity=Severity.WARNING,
                    incident_type=state.incident_type,
                    server_id=state.server_id,
                    rack_id=rack_id,
                    current_val=val,
                    threshold_val=threshold_rule.warning_threshold,
                    unit=threshold_rule.unit,
                    status=IncidentStatus.OPEN,
                )

                event_to_emit = self._build_event(
                    state=state,
                    rack_id=rack_id,
                    event_type=EventType.INCIDENT_OPENED,
                    notification_required=False,
                    message=msg,
                )

        # 3. Transition to RECOVERY / CLOSED
        elif is_recovery:
            if state.phase in ("WARNING", "CRITICAL"):
                state.phase = "RECOVERY"
                state.status = IncidentStatus.RECOVERING

                msg = (
                    f"RECOVERY STARTED: {state.incident_type.value} on server {state.server_id} ({rack_id}). "
                    f"Current value {val:.1f}{threshold_rule.unit} fell below recovery threshold ({threshold_rule.recovery_threshold:.1f}{threshold_rule.unit})."
                )

                event_to_emit = self._build_event(
                    state=state,
                    rack_id=rack_id,
                    event_type=EventType.INCIDENT_RECOVERY_STARTED,
                    notification_required=False,
                    message=msg,
                )
            elif state.phase == "RECOVERY" and state.consecutive_recovery >= self.config.recovery_confirmation_count:
                state.phase = "NORMAL"
                state.status = IncidentStatus.CLOSED

                msg = format_alert_message(
                    severity=state.severity or Severity.WARNING,
                    incident_type=state.incident_type,
                    server_id=state.server_id,
                    rack_id=rack_id,
                    current_val=val,
                    threshold_val=threshold_rule.recovery_threshold,
                    unit=threshold_rule.unit,
                    status=IncidentStatus.CLOSED,
                )

                event_to_emit = self._build_event(
                    state=state,
                    rack_id=rack_id,
                    event_type=EventType.INCIDENT_CLOSED,
                    notification_required=False,
                    message=msg,
                )

                # Reset state session
                state.incident_id = None
                state.has_notified_critical = False
                state.severity = None

        return event_to_emit

    def _build_event(
        self,
        state: ServerIncidentState,
        rack_id: str,
        event_type: EventType,
        notification_required: bool,
        message: str,
    ) -> dict:
        return {
            "incident_id": state.incident_id,
            "timestamp": state.last_seen,
            "server_id": state.server_id,
            "rack_id": rack_id,
            "incident_type": state.incident_type.value,
            "severity": state.severity.value if state.severity else "WARNING",
            "status": state.status.value,
            "event_type": event_type.value,
            "first_seen": state.first_seen,
            "last_seen": state.last_seen,
            "current_value": round(state.current_value, 2),
            "threshold": round(state.threshold_value, 2),
            "notification_required": notification_required,
            "message": message,
        }

    def check_state_timeouts(self, current_timestamp_iso: str) -> List[dict]:
        """
        Scans all open/recovering states and closes any exceeding state_timeout_minutes.
        Prevents zombie incident accumulation if telemetry for a server stops arriving.
        """
        closed_events: List[dict] = []
        try:
            curr_dt = datetime.fromisoformat(current_timestamp_iso.replace("Z", "+00:00"))
        except Exception:
            return closed_events

        timeout_seconds = self.config.state_timeout_minutes * 60

        for key, state in list(self.states.items()):
            if state.phase != "NORMAL" and state.last_seen:
                try:
                    last_dt = datetime.fromisoformat(state.last_seen.replace("Z", "+00:00"))
                    elapsed = (curr_dt - last_dt).total_seconds()
                    if elapsed >= timeout_seconds:
                        # Timeout triggered
                        state.phase = "NORMAL"
                        state.status = IncidentStatus.CLOSED
                        msg = (
                            f"INCIDENT CLOSED (State Timeout): No telemetry received for server {state.server_id} "
                            f"on incident {state.incident_type.value} for over {self.config.state_timeout_minutes} minutes."
                        )
                        evt = self._build_event(
                            state=state,
                            rack_id="UNKNOWN",
                            event_type=EventType.INCIDENT_CLOSED,
                            notification_required=False,
                            message=msg,
                        )
                        closed_events.append(evt)
                        state.incident_id = None
                        state.has_notified_critical = False
                        state.severity = None
                except Exception:
                    continue

        return closed_events
