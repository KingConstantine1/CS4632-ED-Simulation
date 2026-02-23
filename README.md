# CS 4632 – Emergency Department Discrete-Event Simulation

This repository contains a semester project for CS 4632 (Modeling and Simulation).
The project implements a discrete-event simulation (DES) to model patient flow in a hospital Emergency Department (ED).

## Implemented (Milestone 2 – Initial Implementation)
- Poisson arrivals via exponential interarrival times (rate λ)
- Exponential service times per server (rate μ)
- M/M/c core structure with:
  - FIFO waiting queue
  - c parallel servers (staff)
  - Event list maintained as a priority queue (min-heap)
- Event types:
  - ARRIVAL (creates patient, schedules next arrival, routes to server/queue)
  - DEPARTURE (frees server, starts next queued patient if any)
- Metrics collected after warm-up period:
  - average wait time
  - average system time
  - average queue length (time-weighted)
  - max queue length (after warm-up)
  - staff utilization (time-weighted)
  - throughput (patients/hour)

## How to Run
From the repo root:

```bash
python src/des_sim.py --lambda_per_hour 15 --mu_per_min 0.25 --servers 3 --T 480 --Tw 60 --seed 1
