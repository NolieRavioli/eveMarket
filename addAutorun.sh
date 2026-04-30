#!/usr/bin/env bash
# addAutorun.sh — install eveMarket as a systemd service on Ubuntu.
# Must be run as root (sudo ./addAutorun.sh).
set -euo pipefail

SERVICE_NAME="evemarket"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"
VENV_DIR=".venv"

# ── resolve paths ─────────────────────────────────────────────────────────────
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SYSTEM_PYTHON="$(command -v python3)"
if [[ -z "$SYSTEM_PYTHON" ]]; then
    echo "ERROR: python3 not found in PATH" >&2
    exit 1
fi

# Create (or reuse) a local virtualenv so dependency install does not touch
# distro-managed Python (PEP 668 on Debian/Ubuntu).
if [[ ! -x "$SCRIPT_DIR/${VENV_DIR}/bin/python3" ]]; then
    echo "Creating virtual environment at ${SCRIPT_DIR}/${VENV_DIR}..."
    if ! "$SYSTEM_PYTHON" -m venv "$SCRIPT_DIR/${VENV_DIR}"; then
        echo "ERROR: failed to create virtualenv." >&2
        echo "Hint: install the venv package first (e.g. sudo apt install python3-venv)." >&2
        exit 1
    fi
fi
PYTHON="$SCRIPT_DIR/${VENV_DIR}/bin/python3"

# Install Python dependencies.
echo "Installing Python dependencies..."
"$PYTHON" -m pip install --quiet -r "$SCRIPT_DIR/requirements.txt"

# Run as the user who called sudo (fall back to current user).
RUN_AS="${SUDO_USER:-$(id -un)}"

echo "Installing ${SERVICE_NAME} service"
echo "  Working dir : $SCRIPT_DIR"
echo "  Python      : $PYTHON"
echo "  Run as user : $RUN_AS"

# ── write the unit file ───────────────────────────────────────────────────────
cat > "$SERVICE_FILE" <<EOF
[Unit]
Description=eveMarket collector and API
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=${RUN_AS}
WorkingDirectory=${SCRIPT_DIR}
ExecStart=${PYTHON} ${SCRIPT_DIR}/eveMarket.py
Restart=on-failure
RestartSec=15
StandardOutput=journal
StandardError=journal
SyslogIdentifier=${SERVICE_NAME}

[Install]
WantedBy=multi-user.target
EOF

echo "Unit file written to $SERVICE_FILE"

# ── enable and start ──────────────────────────────────────────────────────────
systemctl daemon-reload
systemctl enable "$SERVICE_NAME"
systemctl restart "$SERVICE_NAME"

echo ""
echo "Done. Service status:"
systemctl status "$SERVICE_NAME" --no-pager || true
echo ""
echo "Useful commands:"
echo "  sudo journalctl -u ${SERVICE_NAME} -f   # follow logs"
echo "  sudo systemctl stop ${SERVICE_NAME}     # stop"
echo "  sudo systemctl restart ${SERVICE_NAME}  # restart"
echo "  sudo bash ${SCRIPT_DIR}/rmAutorun.sh    # uninstall"
