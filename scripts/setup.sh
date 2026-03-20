#!/usr/bin/env bash
# scripts/setup.sh
# ─────────────────────────────────────────────────────────────
# Bootstrap the full analytics project in one command.
# Usage: bash scripts/setup.sh [--dsn "postgresql://..."]
# ─────────────────────────────────────────────────────────────

set -euo pipefail

DSN="${2:-postgresql://postgres:postgres@localhost:5432/analytics}"

GREEN='\033[0;32m'; YELLOW='\033[1;33m'; RED='\033[0;31m'; NC='\033[0m'

info()  { echo -e "${GREEN}[✓]${NC} $*"; }
warn()  { echo -e "${YELLOW}[!]${NC} $*"; }
error() { echo -e "${RED}[✗]${NC} $*"; exit 1; }

echo ""
echo "  SQL-Driven Analytics Dashboard — Setup"
echo "  ======================================="
echo ""

# ── 1. Check Python ──────────────────────────────────────────
if ! command -v python3 &>/dev/null; then
  error "Python 3.10+ required. Install from https://python.org"
fi
PY_VER=$(python3 -c 'import sys; print(sys.version_info[:2] >= (3,10))')
[[ "$PY_VER" == "True" ]] || error "Python 3.10+ required (found $(python3 --version))"
info "Python $(python3 --version)"

# ── 2. Virtual environment ───────────────────────────────────
if [[ ! -d .venv ]]; then
  python3 -m venv .venv
  info "Created .venv"
fi
# shellcheck disable=SC1091
source .venv/bin/activate
info "Activated .venv"

# ── 3. Install dependencies ──────────────────────────────────
pip install --quiet --upgrade pip
pip install --quiet -r requirements.txt
info "Dependencies installed"

# ── 4. Check PostgreSQL ──────────────────────────────────────
if ! command -v psql &>/dev/null; then
  warn "psql not found — skipping DB setup. Run manually:"
  warn "  psql -d analytics -f sql/01_schema.sql"
  warn "  psql -d analytics -f sql/02_indexes.sql"
  warn "  psql -d analytics -f sql/03_maintenance.sql"
else
  info "psql found"

  # Create DB if it doesn't exist
  DB_NAME=$(echo "$DSN" | sed 's/.*\///')
  if ! psql -lqt | cut -d \| -f 1 | grep -qw "$DB_NAME"; then
    psql -c "CREATE DATABASE $DB_NAME;" 2>/dev/null || warn "Could not create DB (may already exist)"
  fi

  psql "$DSN" -f sql/01_schema.sql -q && info "Schema applied (01_schema.sql)"
  psql "$DSN" -f sql/02_indexes.sql -q && info "Indexes applied (02_indexes.sql)"
  psql "$DSN" -f sql/03_maintenance.sql -q && info "Maintenance views applied"

  # ── 5. Seed data ───────────────────────────────────────────
  echo ""
  read -r -p "  Seed database with mock data? [y/N] " SEED
  if [[ "$SEED" =~ ^[Yy]$ ]]; then
    python3 scripts/seed_data.py --dsn "$DSN"
    info "Seed complete"
  fi
fi

# ── 6. .env file ─────────────────────────────────────────────
if [[ ! -f .env ]]; then
  cp .env.example .env
  sed -i "s|postgresql://.*|$DSN|" .env
  info ".env created from .env.example"
fi

# ── Done ─────────────────────────────────────────────────────
echo ""
echo "  ─────────────────────────────────────"
info "Setup complete!"
echo ""
echo "  Start the API:"
echo "    source .venv/bin/activate"
echo "    uvicorn api.main:app --reload --port 8000"
echo ""
echo "  Swagger UI:  http://localhost:8000/docs"
echo "  Dashboard:   open frontend/dashboard.html"
echo ""
