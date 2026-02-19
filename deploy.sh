
### /deploy.sh
```bash
#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TARGET="${1:-help}"

case "$TARGET" in
  backend-render)
    echo "Render deployment (git-based):"
    echo "1) git push origin main"
    echo "2) In Render, create Web Service with:"
    echo "   Root Directory: backend"
    echo "   Build Command: pip install -r requirements.txt"
    echo "   Start Command: uvicorn app.main:app --host 0.0.0.0 --port \$PORT"
    echo "3) Set env vars: APP_ENV=production, FRONTEND_ORIGINS=<your vercel url>"
    ;;

  backend-railway)
    cd "$ROOT_DIR/backend"
    railway up
    ;;

  frontend-vercel)
    cd "$ROOT_DIR/frontend"
    npm install
    vercel --prod
    ;;

  local)
    echo "Starting backend and frontend locally..."
    (cd "$ROOT_DIR/backend" && uvicorn app.main:app --host 0.0.0.0 --port 8000) &
    BACKEND_PID=$!
    trap 'kill $BACKEND_PID' EXIT
    cd "$ROOT_DIR/frontend"
    npm install
    npm run dev
    ;;

  *)
    echo "Usage:"
    echo "  ./deploy.sh backend-render"
    echo "  ./deploy.sh backend-railway"
    echo "  ./deploy.sh frontend-vercel"
    echo "  ./deploy.sh local"
    ;;
esac
