# RIFT 2026 Money Muling Detection System

## Project Title
RIFT 2026 Money Muling Detection System

## Live Demo URL
https://rift26-money-mule.vercel.app/
## Tech Stack
- Backend: FastAPI, Pandas, NetworkX
- Frontend: React (Vite), Tailwind CSS, Axios, react-force-graph
- Runtime: Python 3.11.9, Node.js 18+
- Deployment: Render (backend), Vercel (frontend)

## System Architecture
```text
[React Frontend]
  - CSV upload UI
  - Graph visualization
  - Ring and account tables
          |
          | HTTP (multipart/form-data, JSON)
          v
[FastAPI Backend]
  - CSV validation + parsing
  - Preprocessing (degrees, volumes, SCC, time index)
  - Detection modules:
      1) Cycle detection
      2) Smurfing detection
      3) Shell chain detection
      4) Merchant laundering detection
      5) Payroll abuse detection
  - Ring deduplication registry
  - Scoring engine (account + ring risk)
          |
          v
[JSON Result]
  - suspicious_accounts
  - fraud_rings
  - summary
```

## Algorithm Approach (with complexity analysis)
- Preprocessing:
  - Build adjacency, node metrics, time indexes, SCCs.
  - Time complexity: `O(T + E + V)` to `O(T log T)` (sorting timestamps).
- Cycle detection (length 3-5):
  - SCC-pruned, depth-limited DFS, canonical cycle dedupe.
  - Worst-case bounded by SCC-local branching and max depth (5), practical near linear on sparse graphs.
- Smurfing detection:
  - Sliding windows on time-sorted transactions.
  - Time complexity: `O(T)` per grouped stream with two-pointer scans.
- Shell chain detection:
  - Depth-limited traversal with strict intermediary constraints, plus amount/time consistency checks.
  - Time complexity: bounded DFS up to `max_hops`, with deterministic pruning.
- Merchant/payroll detectors:
  - Grouped statistical checks on node-level streams.
  - Time complexity: approximately `O(T log T)` due to sorting/group operations.
- Ring deduplication:
  - `frozenset`-based registry with subset/superset replacement logic.
  - Deterministic, bounded by number of candidate rings.

## Suspicion Score Methodology
- Per-account normalized feature vector in `[0,1]`:
  - `C`: cycle involvement
  - `S`: smurfing involvement
  - `L`: shell involvement
  - `M`: merchant laundering involvement
  - `P`: payroll abuse involvement
  - `V`: velocity anomaly
- Raw risk:
  - `R = 0.30C + 0.20S + 0.15L + 0.15M + 0.15P + 0.05V`
- Role multipliers:
  - organizer `x1.2`, intermediary `x1.0`, destination `x0.9`, passive `x0.8`
- Logistic normalization:
  - `Score = 100 * (1 / (1 + exp(-6 * (R - mu))))`
  - `mu = mean(R)` across accounts
- Output rules:
  - Clamped to `[0,100]`
  - Rounded to 2 decimals
  - Sorted by score desc, account_id asc

## Installation and Setup
### Backend
1. `cd backend`
2. `python -m venv .venv`
3. Activate venv:
   - Windows: `.\.venv\Scripts\Activate.ps1`
   - Linux/macOS: `source .venv/bin/activate`
4. `pip install -r requirements.txt`
5. `uvicorn app.main:app --host 0.0.0.0 --port 8000`

### Frontend
1. `cd frontend`
2. `npm install`
3. Set API base URL:
   - Windows PowerShell: `$env:VITE_API_BASE_URL="http://localhost:8000"`
   - Linux/macOS: `export VITE_API_BASE_URL="http://localhost:8000"`
4. `npm run dev`

## Usage Instructions
1. Open frontend in browser (`http://localhost:5173` by default).
2. Upload a CSV with required columns:
   - `transaction_id,sender_id,receiver_id,amount,timestamp`
3. Run analysis.
4. Review:
   - Suspicious accounts
   - Fraud rings
   - Graph visualization
5. Download result JSON from UI (`/results/download` endpoint).

## Known Limitations
- Detection quality depends on data quality and timestamp accuracy.
- Extremely dense graphs can still increase runtime despite depth and SCC constraints.
- Rule-based constraints may miss novel laundering behavior not reflected in current heuristics.
- Merchant/payroll suppression is heuristic and may require domain-specific threshold tuning.
- Live demo URL in this README is placeholder and should be replaced with actual deployment URL.

## Team Members
- Team Name: Legion
- Risha N
- Prathyush Menon
