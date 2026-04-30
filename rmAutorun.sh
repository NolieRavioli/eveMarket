#!/usr/bin/env bash
# rmAutorun.sh — remove the eveMarket systemd service.
# Must be run as root (sudo ./rmAutorun.sh).
set -euo pipefail

SERVICE_NAME="evemarket"
SERVICE_FILE="/etc/systemd/system/${SERVICE_NAME}.service"

if [[ "$EUID" -ne 0 ]]; then
    echo "ERROR: must be run as root (sudo ./rmAutorun.sh)" >&2
    exit 1
fi

echo "Removing ${SERVICE_NAME} service..."

systemctl stop    "$SERVICE_NAME" 2>/dev/null || true
systemctl disable "$SERVICE_NAME" 2>/dev/null || true

if [[ -f "$SERVICE_FILE" ]]; then
    rm "$SERVICE_FILE"
    echo "Deleted $SERVICE_FILE"
else
    echo "Unit file not found (already removed?): $SERVICE_FILE"
fi

systemctl daemon-reload
systemctl reset-failed 2>/dev/null || true

echo "Done."
