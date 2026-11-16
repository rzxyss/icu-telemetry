"""
Master Demo Launcher for Big Data Module 8 Project:
"ICU Wearable Telemetry Real-Time Processing with Spark Structured Streaming"
"""
import os
import sys
import time
import subprocess
import webbrowser

def main():
    print("=" * 75)
    print("  ICU TELEMETRY REAL-TIME MONITORING SYSTEM")
    print("  Big Data Module 8 - Apache Spark Structured Streaming Case Study")
    print("=" * 75)
    print("Starting Flask Telemetry Web Application...")
    
    # Run app.py using current python executable
    proc = subprocess.Popen([sys.executable, "app.py"])
    
    time.sleep(2)
    url = "http://127.0.0.1:5000"
    print(f"\n>>> Web Application active at: {url}")
    print(">>> Opening browser dashboard automatically...")
    try:
        webbrowser.open(url)
    except Exception:
        pass
        
    print("\n[INFO] To start PySpark Structured Streaming processor concurrently:")
    print("       Open a new terminal and run: python spark_processor.py\n")
    print("Press Ctrl+C to stop the application.\n")
    
    try:
        proc.wait()
    except KeyboardInterrupt:
        print("\nStopping application...")
        proc.terminate()
        proc.wait()
        print("Application stopped.")

if __name__ == "__main__":
    main()
