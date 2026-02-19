# RIFT 2026 Money Muling Detection System

Live Demo URL: https://example.com/rift-2026-demo  
LinkedIn Video URL: https://www.linkedin.com/in/your-team/posts/rift-2026-demo-placeholder

## Overview

Production-ready, deterministic web app for detecting money-muling fraud rings from uploaded CSV transaction data.

- Backend: FastAPI + Pandas + NetworkX
- Frontend: React (Vite) + Tailwind + react-force-graph
- Output: exact JSON schema with deterministic ordering
- Performance target: <= 30s for 10,000 transactions on a small cloud instance

## Architecture

```text
[React + Tailwind + ForceGraph UI]
            |
            | CSV upload (multipart/form-data)
            v
       [FastAPI /analyze]
            |
            +--> strict CSV validator (schema, amount, timestamp, duplicate tx_id)
            |
            +--> cycle detector (SCC-pruned bounded DFS, len 3..5)
            |
            +--> smurfing detector (72h sliding windows, fan-in / fan-out)
            |
            +--> shell detector (bounded DFS, low-activity intermediates)
            |
            +--> scoring engine (+ penalties: merchant/payroll heuristics)
            |
            v
[Exact JSON result + /results/download]
            |
            v
[Graph visualization + ring table + JSON download]
