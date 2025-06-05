#!/bin/bash

# Gaming Reduction Backend - Simulation Runner
# This script runs the SimEngine power simulation with comprehensive logging

# Set script directory and create logs directory
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")

# Default parameters
DEFAULT_SIMS=500
DEFAULT_CORES=16

# Parse command line arguments
SIMS_PARAM=$DEFAULT_SIMS
CORES_PARAM=$DEFAULT_CORES
COMMAND="run"

while [[ $# -gt 0 ]]; do
    case $1 in
        --sims=*)
            SIMS_PARAM="${1#*=}"
            shift
            ;;
        --cores=*)
            CORES_PARAM="${1#*=}"
            shift
            ;;
        run|status|monitor|stop|logs|help)
            COMMAND="$1"
            shift
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use '$0 help' for usage information"
            exit 1
            ;;
    esac
done

# Validate numeric parameters
if ! [[ "$SIMS_PARAM" =~ ^[0-9]+$ ]] || [ "$SIMS_PARAM" -le 0 ]; then
    echo "Warning: Invalid sims parameter '$SIMS_PARAM', using default: $DEFAULT_SIMS"
    SIMS_PARAM=$DEFAULT_SIMS
fi

if ! [[ "$CORES_PARAM" =~ ^[0-9]+$ ]] || [ "$CORES_PARAM" -le 0 ]; then
    echo "Warning: Invalid cores parameter '$CORES_PARAM', using default: $DEFAULT_CORES"
    CORES_PARAM=$DEFAULT_CORES
fi

# Create logs directory if it doesn't exist
mkdir -p "$LOG_DIR"

# Define log files
MAIN_LOG="$LOG_DIR/simulation_${TIMESTAMP}.log"
ERROR_LOG="$LOG_DIR/simulation_error_${TIMESTAMP}.log"
PROGRESS_LOG="$LOG_DIR/simulation_progress_${TIMESTAMP}.log"
PID_FILE="$LOG_DIR/simulation.pid"

# Function to log with timestamp
log_with_timestamp() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$MAIN_LOG"
}

# Function to check if simulation is already running
check_running() {
    if [ -f "$PID_FILE" ]; then
        local pid=$(cat "$PID_FILE")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo "Simulation is already running with PID: $pid"
            echo "Log file: $MAIN_LOG"
            echo "To monitor: tail -f $MAIN_LOG"
            echo "To stop: kill $pid && rm $PID_FILE"
            exit 1
        else
            echo "Removing stale PID file"
            rm "$PID_FILE"
        fi
    fi
}

# Function to cleanup on exit
cleanup() {
    log_with_timestamp "Simulation script exiting, cleaning up..."
    if [ -f "$PID_FILE" ]; then
        rm "$PID_FILE"
    fi
}

# Function to run the simulation
run_simulation() {
    log_with_timestamp "Starting simulation in directory: $PROJECT_ROOT"
    log_with_timestamp "R script: $SCRIPT_DIR/sim_engine_demo.R"
    log_with_timestamp "Simulation parameters: sims=$SIMS_PARAM, cores=$CORES_PARAM"
    log_with_timestamp "Logs directory: $LOG_DIR"
    log_with_timestamp "Main log: $MAIN_LOG"
    log_with_timestamp "Error log: $ERROR_LOG"
    log_with_timestamp "Progress log: $PROGRESS_LOG"
    
    # Store the PID
    echo $$ > "$PID_FILE"
    
    # Set up signal handlers
    trap cleanup EXIT INT TERM
    
    # Change to project directory
    cd "$PROJECT_ROOT"
    
    # Run R script with logging
    log_with_timestamp "Executing R script..."
    
    # Use R CMD BATCH for better output capture and error handling
    # Pass parameters as command line arguments to R script
    R CMD BATCH \
        --no-save \
        --no-restore \
        --args --sims=$SIMS_PARAM --cores=$CORES_PARAM \
        "$SCRIPT_DIR/sim_engine_demo.R" \
        "$PROGRESS_LOG" \
        2>&1 | tee -a "$MAIN_LOG"
    
    # Check R exit status
    R_EXIT_CODE=${PIPESTATUS[0]}
    
    if [ $R_EXIT_CODE -eq 0 ]; then
        log_with_timestamp "✅ Simulation completed successfully!"
        log_with_timestamp "Check the progress log for detailed output: $PROGRESS_LOG"
        
        # Find and report the saved results file
        RESULTS_FILE=$(find "$SCRIPT_DIR" -name "power_sim_results_*.RData" -newer "$PID_FILE" 2>/dev/null | head -1)
        if [ -n "$RESULTS_FILE" ]; then
            log_with_timestamp "📊 Results saved to: $RESULTS_FILE"
        fi
    else
        log_with_timestamp "❌ Simulation failed with exit code: $R_EXIT_CODE"
        log_with_timestamp "Check the progress log for errors: $PROGRESS_LOG"
    fi
    
    # Clean up PID file
    rm "$PID_FILE"
    
    return $R_EXIT_CODE
}

# Function to show monitoring information
show_monitoring_info() {
    echo "=== Simulation Monitoring Information ==="
    echo "Main log:     tail -f $MAIN_LOG"
    echo "Progress log: tail -f $PROGRESS_LOG"
    echo "Error log:    tail -f $ERROR_LOG"
    echo "Check status: ps aux | grep '[R].*sim_engine_demo'"
    echo "Stop simulation: kill \$(cat $PID_FILE) 2>/dev/null && rm $PID_FILE"
    echo "View results: ls -la $SCRIPT_DIR/power_sim_results_*.RData"
    echo "========================================"
}

# Main execution
case "${COMMAND:-run}" in
    "run")
        check_running
        echo "Starting simulation with comprehensive logging..."
        show_monitoring_info
        echo ""
        run_simulation
        ;;
    "status")
        if [ -f "$PID_FILE" ]; then
            pid=$(cat "$PID_FILE")
            if ps -p "$pid" > /dev/null 2>&1; then
                echo "✅ Simulation is running with PID: $pid"
                echo "Started: $(ps -p $pid -o lstart= 2>/dev/null)"
                echo "CPU usage: $(ps -p $pid -o %cpu= 2>/dev/null)%"
                echo "Memory usage: $(ps -p $pid -o %mem= 2>/dev/null)%"
                show_monitoring_info
            else
                echo "❌ Simulation is not running (stale PID file found)"
                rm "$PID_FILE"
            fi
        else
            echo "❌ No simulation is currently running"
        fi
        ;;
    "monitor")
        echo "Monitoring simulation progress (Ctrl+C to exit monitoring)..."
        if [ -f "$PROGRESS_LOG" ]; then
            tail -f "$PROGRESS_LOG"
        else
            echo "No progress log found. Simulation may not be running."
        fi
        ;;
    "stop")
        if [ -f "$PID_FILE" ]; then
            pid=$(cat "$PID_FILE")
            if ps -p "$pid" > /dev/null 2>&1; then
                echo "Stopping simulation with PID: $pid"
                kill "$pid"
                sleep 2
                if ps -p "$pid" > /dev/null 2>&1; then
                    echo "Force killing simulation..."
                    kill -9 "$pid"
                fi
                rm "$PID_FILE"
                echo "✅ Simulation stopped"
            else
                echo "❌ Simulation is not running"
                rm "$PID_FILE"
            fi
        else
            echo "❌ No simulation PID file found"
        fi
        ;;
    "logs")
        echo "Recent log files:"
        ls -la "$LOG_DIR"/*.log 2>/dev/null | tail -10
        echo ""
        echo "To view latest main log: cat $LOG_DIR/simulation_*.log | tail -50"
        echo "To view latest progress log: cat $LOG_DIR/simulation_progress_*.log | tail -50"
        ;;
    "help")
        echo "Gaming Reduction Backend - Simulation Runner"
        echo ""
        echo "Usage: $0 [options] [command]"
        echo ""
        echo "Options:"
        echo "  --sims=N      Number of simulations to run (default: $DEFAULT_SIMS)"
        echo "  --cores=N     Number of CPU cores to use (default: $DEFAULT_CORES)"
        echo ""
        echo "Commands:"
        echo "  run (default) - Start the simulation"
        echo "  status        - Check if simulation is running"
        echo "  monitor       - Monitor simulation progress in real-time"
        echo "  stop          - Stop the running simulation"
        echo "  logs          - List recent log files"
        echo "  help          - Show this help message"
        echo ""
        echo "Examples:"
        echo "  $0 run                           # Run with default parameters"
        echo "  $0 --sims=1000 --cores=8 run    # Run with 1000 sims using 8 cores"
        echo "  $0 --sims=100 run               # Quick test with 100 simulations"
        echo "  $0 status                       # Check if simulation is running"
        echo "  $0 monitor                      # Watch progress in real-time"
        echo ""
        echo "The simulation will run in the background and continue even if you log out."
        ;;
    *)
        echo "Unknown command: $COMMAND"
        echo "Use '$0 help' for usage information"
        exit 1
        ;;
esac 