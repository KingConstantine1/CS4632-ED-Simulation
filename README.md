# CS 4632 Emergency Department Discrete-Event Simulation

Semester project for CS 4632 (Modeling and Simulation).

This repository models patient flow in an Emergency Department (ED) using a discrete-event simulation (DES) with an M/M/c-style queueing structure.

## Project Summary

- Arrival process: Poisson arrivals (exponential interarrival times)
- Service process: exponential service times per server
- Queue discipline: FIFO (first in, first out)
- Servers: `c` parallel service channels
- Core events: `ARRIVAL`, `DEPARTURE`

### Main Metrics (post warm-up)

- Average wait time (minutes)
- Max wait time (minutes)
- Average system time (minutes)
- Average queue length (time-weighted)
- Max queue length (post warm-up)
- Utilization (time-weighted)
- Throughput (patients/hour)
- Arrival/departure counts

## Repository Structure

- `src/des_sim.py`: earlier single-run implementation
- `src/des_sim_m3.py`: milestone 3+ runner with batch matrix support and structured outputs
- `M3_12RunsForMatrix.csv`: milestone experiment matrix
- `m3_outputs/`: milestone output artifacts from 12-run matrix
- `docs/`: placeholder for extra project docs

## Requirements

- Python 3.10+
- Standard library only (no external Python packages required to run simulation)

## Quick Start

From repository root:

```powershell
python src\des_sim_m3.py --matrix_csv M3_12RunsForMatrix.csv --output_dir m3_outputs
```

For a single run, create a config JSON and run:

```powershell
python src\des_sim_m3.py --config_json demo_config.json --output_dir demo_outputs
```

## Single-Run Config Format

Example `demo_config.json`:

```json
{
  "run_id": "DEMO_BASE",
  "lambda_per_hour": 10,
  "mu_per_min": 0.1,
  "servers": 3,
  "T": 480,
  "Tw": 60,
  "seed": 463201,
  "sample_interval_min": 1.0,
  "purpose": "Baseline demo"
}
```

## Parameter Definitions

- `lambda_per_hour`: arrival rate (patients/hour)
- `mu_per_min`: service rate per server (patients/minute/server)
- `servers`: number of parallel servers (`c`)
- `T`: total simulation horizon in minutes
- `Tw`: warm-up duration in minutes (excluded from core metric averaging)
- `seed`: random seed for reproducibility
- `sample_interval_min`: time-series sampling interval in minutes

## Output Artifacts

Each run creates:

- `run_<id>_events.csv`
- `run_<id>_timeseries.csv`
- `run_<id>_summary.json`
- `run_<id>_config.json`

Batch runs also create:

- `run_index.csv`: consolidated status + key metrics across runs

## M/M/c Analytical Comparison

`des_sim_m3.py` computes embedded M/M/c baseline values for stable configurations (`rho < 1`) and stores them in summary JSON:

- `rho`, `P0`, `Lq`, `Wq_min`, `W_min`, `L`

For unstable configurations (`rho >= 1`), analytical values are marked unavailable.

## Reproducibility Notes

- Use fixed seeds for deterministic reruns of a configuration.
- Use multiple replications (different seeds) for statistically robust scenario comparison.
- Keep `Tw` and `T` consistent across compared scenarios unless intentionally testing horizon effects.

## Known Notes for Final Presentation

- Demo files like `demo_*.json` and `demo_outputs/` are generated locally for presentation runs.
- Core grading artifacts are the simulation code, milestone outputs, and analysis/report deliverables.

