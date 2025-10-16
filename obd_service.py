#!/usr/bin/env python3
"""
Standalone OBD-II data logging service with REST API.
Auto-discovers available OBD commands and logs timestamped vehicle data.
"""

import obd
import json
import time
import threading
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, HTTPException
import uvicorn
from collections import deque
import sys

# Add parent directory to path for shared config
sys.path.insert(0, str(Path.home() / "rider-controller"))
from config.logging_config import setup_logger

# Setup logger
logger = setup_logger('obd')

# Configuration
OBD_PORT = None  # Auto-detect, or "/dev/ttyUSB0"
LOG_DIR = Path.home() / "rider-controller" / "logs" / "obd_data"
POLL_INTERVAL = 0.5  # 500ms = 2Hz (as requested)
BUFFER_SIZE = 100  # Keep last 100 readings in memory

# Global state
app = FastAPI(title="OBD Service")
obd_connection = None
current_data = {}
data_buffer = deque(maxlen=BUFFER_SIZE)
is_running = False
current_log_file = None
available_commands = []  # Commands that this vehicle supports

def discover_available_commands():
    """
    Test all OBD commands to see what this vehicle supports.
    This runs once at startup.
    """
    global available_commands
    
    if not obd_connection or not obd_connection.is_connected():
        logger.error("Cannot discover commands - not connected")
        return
    
    logger.info("Discovering available OBD commands (this may take 30+ seconds)...")
    
    # Get all available OBD commands from python-OBD library
    all_commands = obd.commands.modes[1]  # Mode 01 commands (live data)
    
    available_commands = []
    tested = 0
    
    for cmd in all_commands:
        tested += 1
        
        try:
            # Test if command is supported
            response = obd_connection.query(cmd)
            
            if not response.is_null():
                available_commands.append(cmd)
                logger.info(f"  ✓ {cmd.name} - {cmd.desc}")
            
            # Don't spam the ECU
            time.sleep(0.1)
            
        except Exception as e:
            logger.debug(f"  ✗ {cmd.name} - {e}")
    
    logger.info(f"Discovery complete: {len(available_commands)}/{tested} commands available")
    
    # Log the available commands to a file for reference
    discovery_file = LOG_DIR / "available_commands.json"
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    
    commands_info = []
    for cmd in available_commands:
        commands_info.append({
            "name": cmd.name,
            "description": cmd.desc,
            "pid": cmd.pid if hasattr(cmd, 'pid') else None
        })
    
    with open(discovery_file, 'w') as f:
        json.dump({
            "discovered_at": datetime.now().isoformat(),
            "count": len(available_commands),
            "commands": commands_info
        }, f, indent=2)
    
    logger.info(f"Command list saved to: {discovery_file}")

def init_obd():
    """Initialize OBD connection"""
    global obd_connection
    
    try:
        logger.info("Connecting to OBD-II adapter...")
        obd_connection = obd.OBD(OBD_PORT, fast=False)
        
        if obd_connection.is_connected():
            logger.info(f"✓ Connected to {obd_connection.port_name()}")
            logger.info(f"  Protocol: {obd_connection.protocol_name()}")
            logger.info(f"  ECU: {obd_connection.ecus}")
            
            # Discover what commands this vehicle supports
            discover_available_commands()
            
            return True
        else:
            logger.error("✗ Failed to connect to OBD-II adapter")
            return False
            
    except Exception as e:
        logger.error(f"OBD initialization error: {e}")
        return False

def get_log_filename():
    """Generate log filename based on current date"""
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    return LOG_DIR / f"obd_{date_str}.jsonl"

def poll_obd():
    """
    Main OBD polling loop - runs in background thread.
    This is where the actual data collection happens.
    """
    global is_running, current_data, current_log_file
    
    logger.info("OBD polling started")
    
    while is_running:
        try:
            timestamp = datetime.now().isoformat()
            data = {"timestamp": timestamp}
            
            # Query each available OBD command
            for cmd in available_commands:
                try:
                    response = obd_connection.query(cmd)
                    
                    if not response.is_null():
                        # Extract value and unit
                        if hasattr(response.value, 'magnitude'):
                            value = float(response.value.magnitude)
                            unit = str(response.value.units)
                        else:
                            value = float(response.value)
                            unit = ""
                        
                        data[cmd.name] = {
                            "value": value,
                            "unit": unit
                        }
                except Exception as e:
                    logger.debug(f"Error querying {cmd.name}: {e}")
            
            # Update global state
            current_data = data
            data_buffer.append(data)
            
            # Log to file (JSONL format - one JSON object per line)
            log_file = get_log_filename()
            if log_file != current_log_file:
                current_log_file = log_file
                logger.info(f"Logging to: {log_file}")
            
            with open(log_file, 'a') as f:
                f.write(json.dumps(data) + '\n')
            
            # Wait for next poll interval
            time.sleep(POLL_INTERVAL)
            
        except Exception as e:
            logger.error(f"Polling error: {e}")
            time.sleep(1)  # Back off on errors
    
    logger.info("OBD polling stopped")

# ========== REST API ENDPOINTS ==========

@app.get("/")
def root():
    """
    Service status endpoint.
    Shows if OBD is connected and running.
    """
    return {
        "service": "OBD Data Logger",
        "status": "running" if is_running else "stopped",
        "connected": obd_connection.is_connected() if obd_connection else False,
        "port": obd_connection.port_name() if obd_connection and obd_connection.is_connected() else None,
        "available_commands": len(available_commands),
        "poll_interval": POLL_INTERVAL
    }

@app.get("/current")
def get_current():
    """
    Get most recent OBD data snapshot.
    Returns all available sensor readings from last poll.
    """
    if not current_data:
        raise HTTPException(status_code=503, detail="No data available yet")
    return current_data

@app.get("/commands")
def get_available_commands():
    """
    List all OBD commands available on this vehicle.
    Useful for knowing what data you can get.
    """
    commands_list = []
    for cmd in available_commands:
        commands_list.append({
            "name": cmd.name,
            "description": cmd.desc
        })
    
    return {
        "count": len(commands_list),
        "commands": commands_list
    }

@app.get("/history")
def get_history(date: str = None):
    """
    Get historical data for a specific date (YYYY-MM-DD).
    Reads from the JSONL log file for that day.
    """
    if not date:
        date = datetime.now().strftime("%Y-%m-%d")
    
    log_file = LOG_DIR / f"obd_{date}.jsonl"
    if not log_file.exists():
        raise HTTPException(status_code=404, detail=f"No data for {date}")
    
    # Read JSONL file (one JSON object per line)
    data = []
    with open(log_file, 'r') as f:
        for line in f:
            data.append(json.loads(line))
    
    return {
        "date": date,
        "count": len(data),
        "data": data
    }

@app.get("/health")
def get_health():
    """
    Health check endpoint.
    Returns detailed connection and logging status.
    """
    return {
        "service": "obd",
        "healthy": is_running and obd_connection and obd_connection.is_connected(),
        "connected": obd_connection.is_connected() if obd_connection else False,
        "logging": is_running,
        "samples_collected": len(data_buffer),
        "last_sample_age_seconds": (
            (datetime.now() - datetime.fromisoformat(current_data['timestamp'])).total_seconds()
            if current_data and 'timestamp' in current_data
            else None
        )
    }

# ========== STARTUP/SHUTDOWN ==========

@app.on_event("startup")
def startup():
    """
    STARTUP EVENT - Called automatically when FastAPI starts.
    
    This is where initialization happens:
    1. Connect to OBD adapter
    2. Discover available commands
    3. Start polling thread
    
    You don't call this manually - FastAPI calls it when service starts.
    """
    global is_running
    
    logger.info("=" * 60)
    logger.info("OBD Service Starting")
    logger.info("=" * 60)
    
    # Try to connect to OBD adapter
    if init_obd():
        # Connection successful - start automatic polling
        is_running = True
        thread = threading.Thread(target=poll_obd, daemon=True)
        thread.start()
        logger.info("✓ OBD logging started automatically")
    else:
        # Connection failed - service runs in degraded mode
        logger.warning("⚠ OBD not connected - service running in degraded mode")
        logger.warning("  Service will retry connection if you call /start endpoint")

@app.on_event("shutdown")
def shutdown():
    """
    SHUTDOWN EVENT - Called when service stops (Ctrl+C or systemctl stop).
    
    Cleans up resources:
    - Stops polling thread
    - Closes OBD connection
    
    You don't call this manually - FastAPI calls it automatically.
    """
    global is_running
    
    logger.info("OBD Service Shutting Down...")
    
    # Stop polling loop
    is_running = False
    time.sleep(1)  # Give thread time to exit gracefully
    
    # Close OBD connection
    if obd_connection:
        obd_connection.close()
        logger.info("✓ OBD connection closed")
    
    logger.info("✓ Shutdown complete")

# ========== MANUAL CONTROL ENDPOINTS ==========
# These exist if you want to manually start/stop logging without restarting service

@app.post("/start")
def manual_start_logging():
    """
    MANUAL START - Only needed if you want to restart logging after calling /stop.
    
    Normally logging starts automatically on service startup.
    This endpoint lets you restart if you stopped it manually.
    """
    global is_running
    
    if is_running:
        return {"status": "already running"}
    
    # Try to reconnect if connection was lost
    if not obd_connection or not obd_connection.is_connected():
        logger.info("Attempting to reconnect to OBD...")
        if not init_obd():
            raise HTTPException(status_code=503, detail="Cannot connect to OBD adapter")
    
    is_running = True
    thread = threading.Thread(target=poll_obd, daemon=True)
    thread.start()
    
    logger.info("Manual logging start requested")
    return {"status": "started"}

@app.post("/stop")
def manual_stop_logging():
    """
    MANUAL STOP - Temporarily stop logging without shutting down service.
    
    vs SHUTDOWN:
    - /stop: Stops logging, keeps service running, keeps connection open
    - shutdown: Closes everything when service exits
    
    Use /stop if you want to pause logging but keep service alive.
    Service will still respond to API requests, just won't poll OBD.
    """
    global is_running
    
    if not is_running:
        return {"status": "already stopped"}
    
    is_running = False
    logger.info("Manual logging stop requested")
    
    return {"status": "stopped"}

# ========== MAIN ENTRY POINT ==========

if __name__ == "__main__":
    """
    This runs when you execute: python3 obd_service.py
    
    Starts the FastAPI server with uvicorn.
    The startup() function above will be called automatically.
    """
    logger.info("Starting OBD service on port 8001...")
    
    uvicorn.run(
        app,
        host="0.0.0.0",  # Listen on all network interfaces
        port=8001,
        log_level="info"
    )
