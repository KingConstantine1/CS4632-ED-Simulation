"""
CS 4632 - Milestone 2 (Initial Implementation)
Discrete-Event Simulation for Emergency Department patient flow (M/M/c core)

- Arrivals: Poisson process with rate lambda (patients per minute)
  Interarrival ~ Exp(lambda)
- Service times: Exp(mu) per server (patients per minute)
- c parallel servers (staff)
- FIFO queue
- Event list: priority queue ordered by event time
- Termination: run until time T (minutes)
- Warm-up: Tw minutes (statistics collected after Tw)

Run:
  python src/des_sim.py --lambda_per_hour 15 --mu_per_min 0.25 --servers 3 --T 480 --Tw 60 --seed 1
"""

from __future__ import annotations

from dataclasses import dataclass, field
import heapq
import math
import random
from collections import deque
from typing import Deque, List, Optional, Tuple


ARRIVAL = "ARRIVAL"
DEPARTURE = "DEPARTURE"


def exp_sample(rate: float, rng: random.Random) -> float:
    """Sample from Exp(rate). rate must be > 0."""
    if rate <= 0:
        raise ValueError("Exponential rate must be > 0")
    u = rng.random()
    return -math.log(1.0 - u) / rate


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
        # tie-breaker handled externally by pushing (time, counter, Event)
        self.sort_index = 0


class DES_EmergencyDepartment:
    def __init__(
        self,
        lambda_per_min: float,
        mu_per_min: float,
        servers: int,
        T: float,
        Tw: float,
        seed: Optional[int] = None,
    ):
        self.lambda_rate = lambda_per_min
        self.mu_rate = mu_per_min
        self.c = servers
        self.T = T
        self.Tw = Tw

        self.rng = random.Random(seed)

        # System state
        self.clock: float = 0.0
        self.next_pid: int = 1
        self.queue: Deque[int] = deque()

        # Server state: True if busy
        self.server_busy: List[bool] = [False] * self.c
        self.server_current_patient: List[Optional[int]] = [None] * self.c

        # Patient records
        self.patients: dict[int, Patient] = {}

        # Event list: heap of (time, counter, Event)
        self.event_list: List[Tuple[float, int, Event]] = []
        self._counter: int = 0

        # Time-weighted statistics (after warm-up only)
        self._last_event_time: float = 0.0
        self._area_queue: float = 0.0
        self._area_busy_servers: float = 0.0

        # Point statistics (after warm-up only)
        self._wait_times: List[float] = []
        self._system_times: List[float] = []
        self._served_count: int = 0
        self._max_queue_len_after_warmup: int = 0

    # ------------------ helpers ------------------

    def _push_event(self, e: Event) -> None:
        self._counter += 1
        heapq.heappush(self.event_list, (e.time, self._counter, e))

    def _pop_event(self) -> Event:
        _, _, e = heapq.heappop(self.event_list)
        return e

    def _busy_count(self) -> int:
        return sum(1 for b in self.server_busy if b)

    def _find_idle_server(self) -> Optional[int]:
        for i, busy in enumerate(self.server_busy):
            if not busy:
                return i
        return None

    def _update_time_weighted_stats(self, new_time: float) -> None:
        """
        Update areas for queue length and busy servers over (last_time, new_time),
        but only count the portion that occurs after warm-up time Tw.
        """
        old_time = self._last_event_time
        if new_time <= old_time:
            return

        interval_start = old_time
        interval_end = new_time

        # compute overlap with [Tw, +inf)
        start = max(interval_start, self.Tw)
        end = interval_end
        if end <= start:
            self._last_event_time = new_time
            return

        dt = end - start
        q_len = len(self.queue)
        busy = self._busy_count()

        self._area_queue += q_len * dt
        self._area_busy_servers += busy * dt

        if interval_end >= self.Tw:
            self._max_queue_len_after_warmup = max(self._max_queue_len_after_warmup, q_len)

        self._last_event_time = new_time

    # ------------------ event logic ------------------

    def _handle_arrival(self, event_time: float) -> None:
        # Create patient
        pid = self.next_pid
        self.next_pid += 1
        p = Patient(pid=pid, arrival_time=event_time)
        self.patients[pid] = p

        # Immediately schedule next arrival (if within horizon)
        next_arrival = event_time + exp_sample(self.lambda_rate, self.rng)
        if next_arrival <= self.T:
            self._push_event(Event(time=next_arrival, event_type=ARRIVAL))

        # Route patient
        idle = self._find_idle_server()
        if idle is not None:
            self._start_service(pid, idle, event_time)
        else:
            self.queue.append(pid)

    def _start_service(self, pid: int, server_id: int, start_time: float) -> None:
        self.server_busy[server_id] = True
        self.server_current_patient[server_id] = pid

        p = self.patients[pid]
        p.service_start_time = start_time

        service_time = exp_sample(self.mu_rate, self.rng)
        depart_time = start_time + service_time
        self._push_event(Event(time=depart_time, event_type=DEPARTURE, patient_id=pid, server_id=server_id))

    def _handle_departure(self, pid: int, server_id: int, depart_time: float) -> None:
        p = self.patients[pid]
        p.departure_time = depart_time

        # Record metrics only if patient arrived after warm-up
        # (simple, implementable, and acceptable for M2)
        if p.arrival_time >= self.Tw:
            wait = (p.service_start_time - p.arrival_time) if p.service_start_time is not None else 0.0
            system = (p.departure_time - p.arrival_time) if p.departure_time is not None else 0.0
            self._wait_times.append(wait)
            self._system_times.append(system)
            self._served_count += 1

        # Free server
        self.server_busy[server_id] = False
        self.server_current_patient[server_id] = None

        # If queue has someone waiting, start immediately
        if self.queue:
            next_pid = self.queue.popleft()
            self._start_service(next_pid, server_id, depart_time)

    # ------------------ main run ------------------

    def run(self) -> dict:
        # Schedule first arrival at time 0 + Exp(lambda)
        first_arrival = exp_sample(self.lambda_rate, self.rng)
        if first_arrival <= self.T:
            self._push_event(Event(time=first_arrival, event_type=ARRIVAL))

        self._last_event_time = 0.0
        self.clock = 0.0

        while self.event_list:
            e = self._pop_event()
            if e.time > self.T:
                break

            # update time-weighted stats up to this event time
            self._update_time_weighted_stats(e.time)

            self.clock = e.time

            if e.event_type == ARRIVAL:
                self._handle_arrival(self.clock)
            elif e.event_type == DEPARTURE:
                self._handle_departure(e.patient_id, e.server_id, self.clock)
            else:
                raise ValueError(f"Unknown event type: {e.event_type}")

        # Finalize time-weighted stats to horizon T
        self._update_time_weighted_stats(self.T)

        stats_time = max(0.0, self.T - self.Tw)

        avg_q = (self._area_queue / stats_time) if stats_time > 0 else 0.0
        avg_busy = (self._area_busy_servers / stats_time) if stats_time > 0 else 0.0
        utilization = (avg_busy / self.c) if self.c > 0 else 0.0

        avg_wait = sum(self._wait_times) / len(self._wait_times) if self._wait_times else 0.0
        avg_system = sum(self._system_times) / len(self._system_times) if self._system_times else 0.0

        throughput_per_min = (self._served_count / stats_time) if stats_time > 0 else 0.0
        throughput_per_hour = throughput_per_min * 60.0

        return {
            "lambda_per_min": self.lambda_rate,
            "mu_per_min": self.mu_rate,
            "servers": self.c,
            "T_min": self.T,
            "Tw_min": self.Tw,
            "served_after_warmup": self._served_count,
            "avg_wait_min": avg_wait,
            "avg_system_time_min": avg_system,
            "avg_queue_len": avg_q,
            "max_queue_len_after_warmup": self._max_queue_len_after_warmup,
            "utilization": utilization,
            "throughput_per_hour": throughput_per_hour,
        }


def main():
    import argparse

    parser = argparse.ArgumentParser(description="ED Discrete-Event Simulation (M/M/c core)")
    parser.add_argument("--lambda_per_hour", type=float, default=15.0, help="Arrival rate (patients per hour)")
    parser.add_argument("--mu_per_min", type=float, default=0.25, help="Service rate per server (patients per minute)")
    parser.add_argument("--servers", type=int, default=3, help="Number of servers (staff)")
    parser.add_argument("--T", type=float, default=480.0, help="Simulation horizon in minutes")
    parser.add_argument("--Tw", type=float, default=60.0, help="Warm-up period in minutes")
    parser.add_argument("--seed", type=int, default=1, help="Random seed")

    args = parser.parse_args()

    lam_per_min = args.lambda_per_hour / 60.0

    sim = DES_EmergencyDepartment(
        lambda_per_min=lam_per_min,
        mu_per_min=args.mu_per_min,
        servers=args.servers,
        T=args.T,
        Tw=args.Tw,
        seed=args.seed,
    )
    results = sim.run()

    print("\n=== ED DES Results (after warm-up) ===")
    for k, v in results.items():
        if isinstance(v, float):
            print(f"{k:28s}: {v:.4f}")
        else:
            print(f"{k:28s}: {v}")
    print("====================================\n")


if __name__ == "__main__":
    main()
