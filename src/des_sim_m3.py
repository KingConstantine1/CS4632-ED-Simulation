"""
CS 4632 - Milestone 3 (Complete Implementation & Testing)
Emergency Department DES (M/M/c core) with M3-grade data collection outputs.

What this script adds beyond basic M2:
- Config-file and CSV-matrix parameterization
- Input validation and explicit error handling
- Automated data export per run:
  - events CSV
  - time series CSV
  - summary JSON
  - config JSON
- Run index CSV across batch runs
- Optional M/M/c theoretical comparison for stable systems
"""

from __future__ import annotations

from collections import deque
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import argparse
import csv
import heapq
import json
import math
from pathlib import Path
import random
import statistics
import time
from typing import Deque, Dict, List, Optional, Tuple


ARRIVAL = "ARRIVAL"
DEPARTURE = "DEPARTURE"


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()


def exp_sample(rate: float, rng: random.Random) -> float:
    if rate <= 0:
        raise ValueError(f"Exponential rate must be > 0 (got {rate})")
    u = rng.random()
    return -math.log(1.0 - u) / rate


@dataclass
class RunConfig:
    run_id: str
    lambda_per_hour: float
    mu_per_min: float
    servers: int
    T: float
    Tw: float
    seed: int
    sample_interval_min: float = 1.0
    purpose: str = ""

    def validate(self) -> None:
        errs: List[str] = []
        if self.lambda_per_hour <= 0:
            errs.append("lambda_per_hour must be > 0")
        if self.mu_per_min <= 0:
            errs.append("mu_per_min must be > 0")
        if self.servers < 1:
            errs.append("servers must be >= 1")
        if self.T <= 0:
            errs.append("T must be > 0")
        if self.Tw < 0:
            errs.append("Tw must be >= 0")
        if self.Tw >= self.T:
            errs.append("Tw must be smaller than T")
        if self.sample_interval_min <= 0:
            errs.append("sample_interval_min must be > 0")
        if errs:
            raise ValueError("Invalid run configuration: " + "; ".join(errs))


@dataclass
class Patient:
    pid: int
    arrival_time: float
    service_start_time: Optional[float] = None
    departure_time: Optional[float] = None


@dataclass(order=True)
class Event:
    time: float
    sort_index: int = field(init=False, repr=False)
    event_type: str = field(compare=False, default="")
    patient_id: int = field(compare=False, default=-1)
    server_id: int = field(compare=False, default=-1)

    def __post_init__(self):
        self.sort_index = 0


@dataclass
class RunOutputs:
    summary: Dict
    events_rows: List[Dict]
    timeseries_rows: List[Dict]


class DESEmergencyDepartment:
    def __init__(self, cfg: RunConfig):
        cfg.validate()
        self.cfg = cfg
        self.lambda_per_min = cfg.lambda_per_hour / 60.0
        self.mu_per_min = cfg.mu_per_min
        self.c = cfg.servers
        self.T = cfg.T
        self.Tw = cfg.Tw
        self.rng = random.Random(cfg.seed)

        self.clock = 0.0
        self.next_pid = 1
        self.queue: Deque[int] = deque()
        self.server_busy: List[bool] = [False] * self.c
        self.server_current_patient: List[Optional[int]] = [None] * self.c
        self.patients: Dict[int, Patient] = {}
        self.event_list: List[Tuple[float, int, Event]] = []
        self._counter = 0

        self._last_event_time = 0.0
        self._area_queue = 0.0
        self._area_busy_servers = 0.0
        self._wait_times: List[float] = []
        self._system_times: List[float] = []
        self._served_count = 0
        self._arrivals_total = 0
        self._departures_total = 0
        self._max_queue_after_warmup = 0

        self.events_rows: List[Dict] = []
        self.timeseries_rows: List[Dict] = []
        self._next_sample_time = max(0.0, self.Tw)

    def _push_event(self, e: Event) -> None:
        self._counter += 1
        heapq.heappush(self.event_list, (e.time, self._counter, e))

    def _pop_event(self) -> Event:
        _, _, e = heapq.heappop(self.event_list)
        return e

    def _busy_count(self) -> int:
        return sum(1 for busy in self.server_busy if busy)

    def _find_idle_server(self) -> Optional[int]:
        for idx, busy in enumerate(self.server_busy):
            if not busy:
                return idx
        return None

    def _record_timeseries_row(self, t: float) -> None:
        self.timeseries_rows.append(
            {
                "sim_time_min": round(t, 6),
                "queue_len": len(self.queue),
                "busy_servers": self._busy_count(),
                "utilization_instant": round(self._busy_count() / self.c, 6),
                "arrivals_total": self._arrivals_total,
                "departures_total": self._departures_total,
            }
        )

    def _record_event_row(self, etype: str, t: float, patient_id: int, server_id: int) -> None:
        self.events_rows.append(
            {
                "sim_time_min": round(t, 6),
                "event_type": etype,
                "patient_id": patient_id,
                "server_id": server_id,
                "queue_len_after_event": len(self.queue),
                "busy_servers_after_event": self._busy_count(),
            }
        )

    def _sample_until(self, new_time: float) -> None:
        while self._next_sample_time <= new_time and self._next_sample_time <= self.T:
            self._record_timeseries_row(self._next_sample_time)
            self._next_sample_time += self.cfg.sample_interval_min

    def _update_time_weighted_stats(self, new_time: float) -> None:
        old = self._last_event_time
        if new_time <= old:
            return

        self._sample_until(new_time)

        start = max(old, self.Tw)
        end = new_time
        if end > start:
            dt = end - start
            q_len = len(self.queue)
            busy = self._busy_count()
            self._area_queue += q_len * dt
            self._area_busy_servers += busy * dt
            self._max_queue_after_warmup = max(self._max_queue_after_warmup, q_len)

        self._last_event_time = new_time

    def _handle_arrival(self, event_time: float) -> None:
        self._arrivals_total += 1
        pid = self.next_pid
        self.next_pid += 1
        self.patients[pid] = Patient(pid=pid, arrival_time=event_time)

        next_arrival = event_time + exp_sample(self.lambda_per_min, self.rng)
        if next_arrival <= self.T:
            self._push_event(Event(time=next_arrival, event_type=ARRIVAL))

        idle = self._find_idle_server()
        if idle is not None:
            self._start_service(pid, idle, event_time)
        else:
            self.queue.append(pid)

        self._record_event_row(ARRIVAL, event_time, pid, -1)

    def _start_service(self, pid: int, server_id: int, start_time: float) -> None:
        self.server_busy[server_id] = True
        self.server_current_patient[server_id] = pid
        p = self.patients[pid]
        p.service_start_time = start_time
        service_time = exp_sample(self.mu_per_min, self.rng)
        self._push_event(
            Event(
                time=start_time + service_time,
                event_type=DEPARTURE,
                patient_id=pid,
                server_id=server_id,
            )
        )

    def _handle_departure(self, pid: int, server_id: int, depart_time: float) -> None:
        self._departures_total += 1
        p = self.patients[pid]
        p.departure_time = depart_time
        if p.service_start_time is not None and p.service_start_time >= self.Tw:
            wait = p.service_start_time - p.arrival_time
            system = p.departure_time - p.arrival_time
            self._wait_times.append(wait)
            self._system_times.append(system)
            self._served_count += 1

        self.server_busy[server_id] = False
        self.server_current_patient[server_id] = None
        if self.queue:
            next_pid = self.queue.popleft()
            self._start_service(next_pid, server_id, depart_time)

        self._record_event_row(DEPARTURE, depart_time, pid, server_id)

    def run(self) -> RunOutputs:
        first_arrival = exp_sample(self.lambda_per_min, self.rng)
        if first_arrival <= self.T:
            self._push_event(Event(time=first_arrival, event_type=ARRIVAL))

        self._last_event_time = 0.0
        self.clock = 0.0
        wall_start = time.perf_counter()

        while self.event_list:
            e = self._pop_event()
            if e.time > self.T:
                break
            self._update_time_weighted_stats(e.time)
            self.clock = e.time
            if e.event_type == ARRIVAL:
                self._handle_arrival(self.clock)
            elif e.event_type == DEPARTURE:
                self._handle_departure(e.patient_id, e.server_id, self.clock)
            else:
                raise ValueError(f"Unknown event type: {e.event_type}")

        self._update_time_weighted_stats(self.T)
        self._record_timeseries_row(self.T)
        wall_seconds = time.perf_counter() - wall_start

        stats_time = max(0.0, self.T - self.Tw)
        avg_q = (self._area_queue / stats_time) if stats_time > 0 else 0.0
        avg_busy = (self._area_busy_servers / stats_time) if stats_time > 0 else 0.0
        utilization = (avg_busy / self.c) if self.c > 0 else 0.0
        avg_wait = statistics.mean(self._wait_times) if self._wait_times else 0.0
        avg_system = statistics.mean(self._system_times) if self._system_times else 0.0
        max_wait = max(self._wait_times) if self._wait_times else 0.0
        throughput_per_min = (self._served_count / stats_time) if stats_time > 0 else 0.0
        throughput_per_hour = throughput_per_min * 60.0

        analytical = mmc_analytical(
            lambda_per_hour=self.cfg.lambda_per_hour,
            mu_per_hour=self.cfg.mu_per_min * 60.0,
            servers=self.cfg.servers,
        )

        summary = {
            "run_id": self.cfg.run_id,
            "generated_utc": utc_now_iso(),
            "purpose": self.cfg.purpose,
            "parameters": {
                "lambda_per_hour": self.cfg.lambda_per_hour,
                "mu_per_min": self.cfg.mu_per_min,
                "servers": self.cfg.servers,
                "T_min": self.cfg.T,
                "Tw_min": self.cfg.Tw,
                "seed": self.cfg.seed,
                "sample_interval_min": self.cfg.sample_interval_min,
            },
            "execution": {
                "wall_clock_seconds": round(wall_seconds, 6),
                "status": "complete",
            },
            "metrics": {
                "served_after_warmup": self._served_count,
                "arrivals_total": self._arrivals_total,
                "departures_total": self._departures_total,
                "avg_wait_min": round(avg_wait, 6),
                "max_wait_min": round(max_wait, 6),
                "avg_system_time_min": round(avg_system, 6),
                "avg_queue_len": round(avg_q, 6),
                "max_queue_len_after_warmup": self._max_queue_after_warmup,
                "utilization": round(utilization, 6),
                "throughput_per_hour": round(throughput_per_hour, 6),
            },
            "analytic_mmc_comparison": analytical,
        }
        return RunOutputs(summary=summary, events_rows=self.events_rows, timeseries_rows=self.timeseries_rows)


def mmc_analytical(lambda_per_hour: float, mu_per_hour: float, servers: int) -> Dict:
    if servers < 1 or lambda_per_hour <= 0 or mu_per_hour <= 0:
        return {"available": False, "reason": "invalid_parameters"}
    a = lambda_per_hour / mu_per_hour
    rho = lambda_per_hour / (servers * mu_per_hour)
    if rho >= 1:
        return {"available": False, "reason": "unstable_system_rho_ge_1", "rho": round(rho, 6)}

    sum_terms = sum((a ** n) / math.factorial(n) for n in range(servers))
    tail = (a**servers) / (math.factorial(servers) * (1 - rho))
    p0 = 1.0 / (sum_terms + tail)
    lq = (p0 * (a**servers) * rho) / (math.factorial(servers) * ((1 - rho) ** 2))
    wq_hours = lq / lambda_per_hour
    w_hours = wq_hours + (1 / mu_per_hour)
    l = lambda_per_hour * w_hours
    return {
        "available": True,
        "rho": round(rho, 6),
        "P0": round(p0, 6),
        "Lq": round(lq, 6),
        "Wq_min": round(wq_hours * 60.0, 6),
        "W_min": round(w_hours * 60.0, 6),
        "L": round(l, 6),
    }


def write_csv(path: Path, rows: List[Dict], headers: List[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=headers)
        w.writeheader()
        for r in rows:
            w.writerow(r)


def write_json(path: Path, payload: Dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2), encoding="utf-8")


def load_runs_from_matrix_csv(path: Path, default_sample_interval: float) -> List[RunConfig]:
    if not path.exists():
        raise FileNotFoundError(f"Run matrix CSV not found: {path}")
    runs: List[RunConfig] = []
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        required = {
            "run_id",
            "arrival_rate_per_hour",
            "service_rate_per_hour",
            "servers",
            "sim_duration_minutes",
            "warmup_minutes",
            "seed",
            "purpose",
        }
        missing = required - set(reader.fieldnames or [])
        if missing:
            raise ValueError(f"Run matrix missing required columns: {sorted(missing)}")
        for row in reader:
            service_rate_per_hour = float(row["service_rate_per_hour"])
            mu_per_min = service_rate_per_hour / 60.0
            runs.append(
                RunConfig(
                    run_id=row["run_id"].strip(),
                    lambda_per_hour=float(row["arrival_rate_per_hour"]),
                    mu_per_min=mu_per_min,
                    servers=int(row["servers"]),
                    T=float(row["sim_duration_minutes"]),
                    Tw=float(row["warmup_minutes"]),
                    seed=int(row["seed"]),
                    sample_interval_min=default_sample_interval,
                    purpose=row.get("purpose", "").strip(),
                )
            )
    return runs


def load_single_run_config_json(path: Path) -> RunConfig:
    if not path.exists():
        raise FileNotFoundError(f"Config JSON not found: {path}")
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {path}: {e}") from e

    required = ["run_id", "lambda_per_hour", "mu_per_min", "servers", "T", "Tw", "seed"]
    missing = [k for k in required if k not in data]
    if missing:
        raise ValueError(f"Config JSON missing keys: {missing}")

    return RunConfig(
        run_id=str(data["run_id"]),
        lambda_per_hour=float(data["lambda_per_hour"]),
        mu_per_min=float(data["mu_per_min"]),
        servers=int(data["servers"]),
        T=float(data["T"]),
        Tw=float(data["Tw"]),
        seed=int(data["seed"]),
        sample_interval_min=float(data.get("sample_interval_min", 1.0)),
        purpose=str(data.get("purpose", "")),
    )


def execute_run(cfg: RunConfig, out_dir: Path) -> Dict:
    sim = DESEmergencyDepartment(cfg)
    outputs = sim.run()

    run_dir = out_dir / cfg.run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    events_path = run_dir / f"run_{cfg.run_id}_events.csv"
    timeseries_path = run_dir / f"run_{cfg.run_id}_timeseries.csv"
    summary_path = run_dir / f"run_{cfg.run_id}_summary.json"
    config_path = run_dir / f"run_{cfg.run_id}_config.json"

    write_csv(
        events_path,
        outputs.events_rows,
        ["sim_time_min", "event_type", "patient_id", "server_id", "queue_len_after_event", "busy_servers_after_event"],
    )
    write_csv(
        timeseries_path,
        outputs.timeseries_rows,
        ["sim_time_min", "queue_len", "busy_servers", "utilization_instant", "arrivals_total", "departures_total"],
    )
    write_json(summary_path, outputs.summary)
    write_json(config_path, asdict(cfg))

    return {
        "run_id": cfg.run_id,
        "purpose": cfg.purpose,
        "status": "complete",
        "events_file": str(events_path),
        "timeseries_file": str(timeseries_path),
        "summary_file": str(summary_path),
        "config_file": str(config_path),
        "duration_sec": outputs.summary["execution"]["wall_clock_seconds"],
        "avg_wait_min": outputs.summary["metrics"]["avg_wait_min"],
        "avg_queue_len": outputs.summary["metrics"]["avg_queue_len"],
        "utilization": outputs.summary["metrics"]["utilization"],
        "throughput_per_hour": outputs.summary["metrics"]["throughput_per_hour"],
    }


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description="M3 ED DES runner with automated data outputs.")
    p.add_argument("--output_dir", type=str, default="m3_outputs", help="Directory for run artifacts")
    p.add_argument("--sample_interval_min", type=float, default=1.0, help="Time-series sampling interval")

    g = p.add_mutually_exclusive_group(required=True)
    g.add_argument("--config_json", type=str, help="Single-run config JSON")
    g.add_argument("--matrix_csv", type=str, help="Batch-run matrix CSV")
    return p


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    run_summaries: List[Dict] = []
    if args.config_json:
        cfg = load_single_run_config_json(Path(args.config_json))
        cfg.sample_interval_min = args.sample_interval_min
        cfg.validate()
        run_summaries.append(execute_run(cfg, out_dir))
    elif args.matrix_csv:
        runs = load_runs_from_matrix_csv(Path(args.matrix_csv), args.sample_interval_min)
        for cfg in runs:
            cfg.validate()
            run_summaries.append(execute_run(cfg, out_dir))
    else:
        raise RuntimeError("Either --config_json or --matrix_csv must be provided.")

    index_path = out_dir / "run_index.csv"
    write_csv(
        index_path,
        run_summaries,
        [
            "run_id",
            "purpose",
            "status",
            "events_file",
            "timeseries_file",
            "summary_file",
            "config_file",
            "duration_sec",
            "avg_wait_min",
            "avg_queue_len",
            "utilization",
            "throughput_per_hour",
        ],
    )
    print(f"Completed {len(run_summaries)} run(s). Index: {index_path}")


if __name__ == "__main__":
    main()

