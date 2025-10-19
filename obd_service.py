#!/usr/bin/env python3
"""
Standalone OBD-II data logging service with REST API.
Auto-discovers available OBD commands and logs timestamped vehicle data.
NEVER GIVES UP - Automatically reconnects forever if connection is lost.
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
MIN_RETRY_DELAY = 5  # Minimum retry delay
MAX_RETRY_DELAY = 30  # Maximum retry delay (can increase if needed)

# Global state
app = FastAPI(title="OBD Service")
obd_connection = None
current_data = {}
data_buffer = deque(maxlen=BUFFER_SIZE)
is_running = False
current_log_file = None
available_commands = []  # Commands that this vehicle supports

# Connection tracking
connection_attempts = 0
last_connection_attempt = None
connection_state = "initializing"  # initializing, connecting, connected, disconnected, reconnecting
last_successful_connection = None

def get_retry_delay():
    """
    Calculate retry delay with exponential backoff (caps at MAX_RETRY_DELAY).
    Starts at MIN_RETRY_DELAY, doubles each time, up to MAX_RETRY_DELAY.
    """
    global connection_attempts
    
    # Exponential backoff: 5s, 10s, 20s, 30s, 30s, 30s...
    delay = min(MIN_RETRY_DELAY * (2 ** min(connection_attempts - 1, 3)), MAX_RETRY_DELAY)
    return delay

def discover_available_commands():
    """
    Test all OBD commands to see what this vehicle supports.
    This runs in background thread after connection.
    """
    global available_commands, connection_state
    
    if not obd_connection or not obd_connection.is_connected():
        logger.error("Cannot discover commands - not connected")
        return
    
    connection_state = "discovering"
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
    connection_state = "connected"

def init_obd():
    """Initialize OBD connection (fast - no discovery)"""
    global obd_connection, connection_attempts, last_connection_attempt, connection_state
    
    connection_attempts += 1
    last_connection_attempt = datetime.now()
    connection_state = "connecting"
    
    try:
        logger.info(f"Connection attempt #{connection_attempts}...")
        logger.info("Connecting to OBD-II adapter...")
        
        # Close old connection if exists
        if obd_connection:
            try:
                obd_connection.close()
            except Exception as e:
                logger.debug(f"Error closing old connection: {e}")
        
        obd_connection = obd.OBD(OBD_PORT, fast=False)
        
        if obd_connection.is_connected():
            logger.info(f"✓ Connected to {obd_connection.port_name()}")
            logger.info(f"  Protocol: {obd_connection.protocol_name()}")
            connection_state = "connected"
            
            # Don't discover commands here - do it in background thread
            # This makes connection instant instead of 30+ seconds
            
            return True
        else:
            logger.warning("✗ Failed to connect to OBD-II adapter")
            connection_state = "disconnected"
            return False
            
    except Exception as e:
        logger.error(f"OBD initialization error: {e}")
        connection_state = "disconnected"
        return False

def discover_and_start_polling():
    """
    Discover commands and start polling (runs in background thread).
    This is split from init_obd() so connection is instant.
    """
    global is_running, last_successful_connection
    
    # Discover what commands this vehicle supports
    discover_available_commands()
    
    # Mark successful connection
    last_successful_connection = datetime.now()
    
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
    NEVER EXITS - Always triggers reconnection if connection lost.
    """
    global is_running, current_data, current_log_file, obd_connection, connection_state
    
    logger.info("OBD polling started")
    consecutive_errors = 0
    max_consecutive_errors = 10  # Reconnect after 10 failed polls
    
    while is_running:
        try:
            # Check connection health
            if not obd_connection or not obd_connection.is_connected():
                logger.error("Lost OBD connection during polling")
                connection_state = "disconnected"
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
                connection_state = "connected"
                
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
                    connection_state = "disconnected"
                    break
            
            # Wait for next poll interval
            time.sleep(POLL_INTERVAL)
            
        except Exception as e:
            consecutive_errors += 1
            logger.error(f"Polling error: {e} (errors: {consecutive_errors}/{max_consecutive_errors})")
            
            if consecutive_errors >= max_consecutive_errors:
                logger.error("Too many consecutive errors - stopping polling")
                connection_state = "disconnected"
                break
            
            time.sleep(1)  # Back off on errors
    
    # Connection lost - trigger reconnection
    is_running = False
    connection_state = "reconnecting"
    logger.info("OBD polling stopped - will attempt reconnection")
    
    # Schedule reconnection in background
    asyncio.run(schedule_reconnection())

async def schedule_reconnection():
    """Schedule reconnection attempt after delay"""
    retry_delay = get_retry_delay()
    logger.info(f"Scheduling reconnection in {retry_delay}s...")
    await asyncio.sleep(retry_delay)
    asyncio.create_task(reconnect_obd())

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
        "poll_interval": POLL_INTERVAL,
        "connection_state": connection_state,
        "connection_attempts": connection_attempts,
        "last_attempt": last_connection_attempt.isoformat() if last_connection_attempt else None,
        "last_successful": last_successful_connection.isoformat() if last_successful_connection else None
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
        ),
        "connection_state": connection_state,
        "connection_attempts": connection_attempts,
        "time_since_last_attempt": (
            (datetime.now() - last_connection_attempt).total_seconds()
            if last_connection_attempt else None
        ),
        "time_since_last_success": (
            (datetime.now() - last_successful_connection).total_seconds()
            if last_successful_connection else None
        )
    }

@app.get("/connection/status")
def get_connection_status():
    """Detailed connection status and retry information"""
    next_retry_in = None
    
    if connection_state in ["disconnected", "reconnecting"] and last_connection_attempt:
        elapsed = (datetime.now() - last_connection_attempt).total_seconds()
        retry_delay = get_retry_delay()
        next_retry_in = max(0, retry_delay - elapsed)
    
    return {
        "state": connection_state,
        "connected": obd_connection.is_connected() if obd_connection else False,
        "attempts": connection_attempts,
        "last_attempt": last_connection_attempt.isoformat() if last_connection_attempt else None,
        "last_success": last_successful_connection.isoformat() if last_successful_connection else None,
        "next_retry_in_seconds": next_retry_in,
        "retry_policy": {
            "min_delay": MIN_RETRY_DELAY,
            "max_delay": MAX_RETRY_DELAY,
            "current_delay": get_retry_delay(),
            "strategy": "exponential_backoff"
        }
    }

# ========== STARTUP/SHUTDOWN ==========

async def initialize_obd_background():
    """
    Initialize OBD in background after server starts.
    NEVER STOPS TRYING - Keeps trying forever if connection fails.
    """
    global is_running, connection_state
    
    # Give server time to fully start
    await asyncio.sleep(2)
    
    logger.info("=" * 60)
    logger.info("OBD Connection Manager Started")
    logger.info("Will keep trying to connect indefinitely")
    logger.info("=" * 60)
    
    # Infinite retry loop
    while True:
        if connection_attempts == 0:
            logger.info("Starting initial OBD connection attempt...")
        else:
            retry_delay = get_retry_delay()
            logger.info(f"Retry #{connection_attempts} in {retry_delay}s...")
            logger.info(f"  (Exponential backoff: {MIN_RETRY_DELAY}s → {MAX_RETRY_DELAY}s)")
            await asyncio.sleep(retry_delay)
        
        # Try to connect to OBD adapter
        if init_obd():
            # Connection successful - reset attempt counter for future reconnections
            logger.info("✓ Connected! Starting command discovery in background...")
            
            # Start discovery and polling in background thread
            thread = threading.Thread(
                target=discover_and_start_polling, 
                daemon=True, 
                name="OBD-Discovery"
            )
            thread.start()
            
            logger.info("✓ OBD service ready (discovery running in background)")
            
            # Wait for this thread to signal disconnection
            while connection_state not in ["disconnected", "reconnecting"]:
                await asyncio.sleep(1)
            
            # Connection lost - continue retry loop
            logger.warning("⚠ Connection lost - will retry")
            
        else:
            # Connection failed
            if connection_attempts == 1:
                logger.warning("⚠ Initial connection failed")
                logger.warning("  Common causes:")
                logger.warning("    • Vehicle ignition is OFF")
                logger.warning("    • OBD adapter not plugged in")
                logger.warning("    • USB cable issue")
                logger.warning("  Will keep trying automatically...")
            else:
                logger.warning(f"⚠ Connection attempt #{connection_attempts} failed")

async def reconnect_obd():
    """
    Attempt to reconnect after connection loss.
    This is called from poll_obd() when connection is lost.
    """
    global is_running, obd_connection, connection_attempts
    
    logger.info("Reconnection triggered after connection loss")
    
    # Close old connection if it exists
    if obd_connection:
        try:
            obd_connection.close()
            logger.info("Closed previous connection")
        except Exception as e:
            logger.debug(f"Error closing connection: {e}")
    
    # Reset for new connection attempt
    obd_connection = None
    
    # Try to reconnect (will continue indefinitely via initialize_obd_background loop)
    retry_delay = get_retry_delay()
    logger.info(f"Will attempt reconnection in {retry_delay}s...")
    await asyncio.sleep(retry_delay)
    
    if init_obd():
        logger.info("✓ Reconnected! Starting command discovery in background...")
        thread = threading.Thread(
            target=discover_and_start_polling, 
            daemon=True, 
            name="OBD-Discovery"
        )
        thread.start()
        logger.info("✓ Reconnection successful")
    else:
        logger.warning("Reconnection failed - will try again")
        # Schedule another attempt
        await asyncio.sleep(get_retry_delay())
        asyncio.create_task(reconnect_obd())

@app.on_event("startup")
async def startup():
    """FastAPI startup - server starts first, then OBD connects in background"""
    logger.info("=" * 60)
    logger.info("OBD Service Starting")
    logger.info("=" * 60)
    
    # Start OBD initialization in background task
    # This will NEVER STOP trying to connect
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
    Manually trigger immediate reconnection attempt.
    Bypasses exponential backoff delay.
    """
    global is_running, connection_attempts
    
    if is_running:
        return {"status": "already running"}
    
    # Reset backoff counter for immediate retry
    logger.info("Manual reconnection requested - attempting immediately")
    connection_attempts = 0
    
    asyncio.create_task(reconnect_obd())
    
    return {
        "status": "reconnection_initiated",
        "message": "Attempting to reconnect to OBD adapter immediately..."
    }

@app.post("/stop")
async def manual_stop_logging():
    """
    Temporarily stop logging without shutting down service.
    Connection stays open but polling stops.
    Auto-reconnection is DISABLED when manually stopped.
    """
    global is_running
    
    if not is_running:
        return {"status": "already stopped"}
    
    is_running = False
    logger.info("Manual logging stop requested - auto-reconnection DISABLED")
    logger.info("Call POST /start to resume")
    
    return {
        "status": "stopped",
        "message": "Logging stopped. Auto-reconnection disabled. Call POST /start to resume."
    }

# ========== MAIN ENTRY POINT ==========

if __name__ == "__main__":
    """
    Main entry point - starts FastAPI server immediately.
    OBD initialization happens in background and NEVER GIVES UP.
    """
    logger.info("=" * 60)
    logger.info("OBD Service with Infinite Retry")
    logger.info("Starting on port 8001...")
    logger.info("=" * 60)
    
    uvicorn.run(
        app,
        host="0.0.0.0",
        port=8001,
        log_level="info"
    )
