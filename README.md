# OBD Service

Standalone OBD-II data logging service for vehicle diagnostics.

## Features
- Auto-discovers available OBD commands
- Logs timestamped data to JSONL files
- REST API for real-time data access
- 2Hz polling rate

## Setup
```bash
# Create virtual environment
python3 -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Add user to dialout group for serial port access
sudo usermod -a -G dialout $USER
sudo reboot

# Test run
python3 obd_service.py
```

## API Endpoints

- `GET /` - Service status
- `GET /current` - Current OBD data snapshot
- `GET /commands` - List available OBD commands
- `GET /health` - Health check
- `GET /history?date=YYYY-MM-DD` - Historical data
- `POST /start` - Manually start logging
- `POST /stop` - Manually stop logging

## Configuration

Edit `obd_service.py` to change:
- `OBD_PORT` - Serial port (default: auto-detect)
- `POLL_INTERVAL` - Polling frequency (default: 0.5s = 2Hz)
- `LOG_DIR` - Log file directory
- `BUFFER_SIZE` - In-memory buffer size

## Logs

- Service logs: `~/rider-controller/logs/obd.log`
- Data logs: `~/rider-controller/logs/obd_data/obd_YYYY-MM-DD.jsonl`
- Command list: `~/rider-controller/logs/obd_data/available_commands.json`

## Systemd Service

Service runs automatically via systemd at `/etc/systemd/system/obd-service.service`
```bash
# View logs
sudo journalctl -u obd-service -f

# Restart service
sudo systemctl restart obd-service

# Check status
sudo systemctl status obd-service
```
