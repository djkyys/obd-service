#!/usr/bin/env python3
"""
Standalone OBD-II data logging service with REST API.
Auto-discovers available OBD commands and logs timestamped vehicle data.
Automatically reconnects if connection is lost.
Discovery runs in background thread for instant startup.
"""

import obd
import json
import time
import threading
import asyncio
import logging
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, HTTPException
import uvicorn
from collections import deque
import sys

# Silence python-OBD's verbose logging (must be before other imports)
obd.logger.setLevel(logging.ERROR)

# Add parent directory to path for shared config
sys.path.insert(0, str(Path.home() / "rider-controller"))
from config.logging_config import setup_logger

# Setup logger with different name to avoid conflict with python-obd logger
logger = setup_logger('obd_service')

# Configuration
OBD_PORT = None  # Auto-detect, or "/dev/ttyUSB0"
LOG_DIR = Path.home() / "rider-controller" / "logs" / "obd_data"
POLL_INTERVAL = 0.5  # 500ms = 2Hz
BUFFER_SIZE = 100  # Keep last 100 readings in memory
RETRY_DELAY = 10  # Seconds between reconnection attempts

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
    This runs in background thread after connection.
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
    """Initialize OBD connection (fast - no discovery)"""
    global obd_connection
    
    try:
        logger.info("Connecting to OBD-II adapter...")
        obd_connection = obd.OBD(OBD_PORT, fast=False)
        
        if obd_connection.is_connected():
            logger.info(f"✓ Connected to {obd_connection.port_name()}")
            logger.info(f"  Protocol: {obd_connection.protocol_name()}")
            
            # Don't discover commands here - do it in background thread
            # This makes connection instant instead of 30+ seconds
            
            return True
        else:
            logger.error("✗ Failed to connect to OBD-II adapter")
            return False
            
    except Exception as e:
        logger.error(f"OBD initialization error: {e}")
        return False

def discover_and_start_polling():
    """
    Discover commands and start polling (runs in background thread).
    This is split from init_obd() so connection is instant.
    """
    global is_running
    
    # Discover what commands this vehicle supports
    discover_available_commands()
    
    # Start polling
    is_running = True
    poll_obd()  # This will run in this thread

def get_log_filename():
    """Generate log filename based on current date"""
    now = datetime.now()
    date_str = now.strftime("%Y-%m-%d")
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    return LOG_DIR / f"obd_{date_str}.jsonl"

def poll_obd():
    """
    Main OBD polling loop - runs in background thread.
    Monitors connection health and triggers reconnection if needed.
    """
    global is_running, current_data, current_log_file, obd_connection
    
    logger.info("OBD polling started")
    consecutive_errors = 0
    max_consecutive_errors = 10  # Reconnect after 10 failed polls
    
    while is_running:
        try:
            # Check connection health
            if not obd_connection or not obd_connection.is_connected():
                logger.error("Lost OBD connection during polling")
                break
            
            timestamp = datetime.now().isoformat()
            data = {"timestamp": timestamp}
            poll_success = False
            
            # Query each available OBD command
            for cmd in available_commands:
                try:
                    response = obd_connection.query(cmd)
                    
                    if not response.is_null():
                        poll_success = True
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
            
            # Check if we got any data
            if poll_success:
                consecutive_errors = 0
                
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
            else:
                consecutive_errors += 1
                logger.warning(f"No valid OBD data received (errors: {consecutive_errors}/{max_consecutive_errors})")
                
                if consecutive_errors >= max_consecutive_errors:
                    logger.error("Too many consecutive errors - connection may be lost")
                    break
            
            # Wait for next poll interval
            time.sleep(POLL_INTERVAL)
            
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"Polling error: {e} (errors: {consecutive_errors}/{max_consecutive_errors})")
            
            if consecutive_errors >= max_consecutive_errors:
                logger.error("Too many consecutive errors - stopping polling")
                break
            
            time.sleep(1)  # Back off on errors
    
    # Cleanup on exit
    is_running = False
    logger.info("OBD polling stopped")

# ========== REST API ENDPOINTS ==========

@app.get("/")
def root():
    """Service status endpoint"""
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
    """Get most recent OBD data snapshot"""
    if not current_data:
        raise HTTPException(status_code=503, detail="No data available yet")
    return current_data

@app.get("/commands")
def get_available_commands_endpoint():
    """List all OBD commands available on this vehicle"""
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
    """Get historical data for a specific date (YYYY-MM-DD)"""
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
    """Health check endpoint"""
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

async def initialize_obd_background():
    """
    Initialize OBD in background after server starts.
    Keeps trying to reconnect if connection fails.
    """
    global is_running
    
    # Give server time to fully start
    await asyncio.sleep(2)
    
    retry_count = 0
    
    while True:
        retry_count += 1
        
        if retry_count == 1:
            logger.info("Starting OBD initialization...")
        else:
            logger.info(f"Reconnection attempt #{retry_count}...")
        
        # Try to connect to OBD adapter
        if init_obd():
            # Connection successful - now discover commands and start polling in background thread
            logger.info("✓ Connected! Starting command discovery in background...")
            thread = threading.Thread(target=discover_and_start_polling, daemon=True, name="OBD-Discovery")
            thread.start()
            logger.info("✓ OBD service ready (discovery running in background)")
            break  # Exit retry loop
        else:
            # Connection failed
            if retry_count == 1:
                logger.warning("⚠ OBD not connected - will retry automatically")
                logger.warning(f"  (Common causes: ignition off, adapter unplugged)")
            else:
                logger.warning(f"⚠ Connection failed - retry #{retry_count}")
            
            # Wait before next retry
            logger.info(f"Waiting {RETRY_DELAY}s before next attempt...")
            await asyncio.sleep(RETRY_DELAY)

async def reconnect_obd():
    """Attempt to reconnect after connection loss"""
    global is_running, obd_connection
    
    logger.info(f"Will attempt reconnection in {RETRY_DELAY}s...")
    await asyncio.sleep(RETRY_DELAY)
    
    # Close old connection if it exists
    if obd_connection:
        try:
            obd_connection.close()
            logger.info("Closed previous connection")
        except Exception as e:
            logger.debug(f"Error closing connection: {e}")
    
    # Try to reconnect
    logger.info("Attempting to reconnect to OBD...")
    if init_obd():
        logger.info("✓ Reconnected! Starting command discovery in background...")
        thread = threading.Thread(target=discover_and_start_polling, daemon=True, name="OBD-Discovery")
        thread.start()
        logger.info("✓ Reconnection successful")
    else:
        logger.warning("Reconnection failed - will try again")
        await asyncio.sleep(RETRY_DELAY)
        asyncio.create_task(reconnect_obd())

@app.on_event("startup")
async def startup():
    """FastAPI startup - server starts first, then OBD connects in background"""
    logger.info("=" * 60)
    logger.info("OBD Service Starting")
    logger.info("=" * 60)
    
    # Start OBD initialization in background task
    asyncio.create_task(initialize_obd_background())

@app.on_event("shutdown")
def shutdown():
    """FastAPI shutdown - cleanup resources"""
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

@app.post("/start")
async def manual_start_logging():
    """
    Manually trigger reconnection attempt.
    Use this to force immediate reconnection without waiting.
    """
    global is_running
    
    if is_running:
        return {"status": "already running"}
    
    # Trigger immediate reconnection attempt
    logger.info("Manual reconnection requested")
    asyncio.create_task(reconnect_obd())
    
    return {
        "status": "reconnection_initiated",
        "message": "Attempting to reconnect to OBD adapter..."
    }

@app.post("/stop")
async def manual_stop_logging():
    """
    Temporarily stop logging without shutting down service.
    Connection stays open but polling stops.
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
    Main entry point - starts FastAPI server immediately.
    OBD initialization happens in background.
    """
    logger.info("Starting OBD service on port 8001...")
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        log_level="info"
    )
