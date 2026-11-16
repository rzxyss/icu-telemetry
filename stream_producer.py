import os
import sys
import time
import json
import socket
import threading
import pandas as pd
import config
from streaming_bridge import bridge

class VitalSignsStreamProducer:
    """
    Simulates high-frequency ICU telemetry wearable devices streaming
    vital signs data over a TCP socket (port 9999) as taught in Module 8.
    """
    def __init__(self, csv_path=config.DATASET_PATH, host=config.SOCKET_HOST, port=config.SOCKET_PORT):
        self.csv_path = csv_path
        self.host = host
        self.port = port
        self.running = False
        self.paused = False
        self.delay_sec = 0.5  # default 2 records per sec
        self.clients = []
        self.clients_lock = threading.Lock()
        self.server_socket = None

    def start_socket_server(self):
        """Starts background TCP socket server for Spark Structured Streaming."""
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.bind((self.host, self.port))
            self.server_socket.listen(5)
            print(f"[PRODUCER] TCP Socket Server listening on {self.host}:{self.port} (Ready for Spark)")
            
            accept_thread = threading.Thread(target=self._accept_loop, daemon=True)
            accept_thread.start()
        except Exception as e:
            print(f"[PRODUCER ERROR] Failed to bind socket {self.host}:{self.port} - {e}")

    def _accept_loop(self):
        while self.running:
            try:
                client_sock, addr = self.server_socket.accept()
                print(f"[PRODUCER] Spark Streaming worker connected from {addr}")
                with self.clients_lock:
                    self.clients.append(client_sock)
            except Exception:
                break

    def broadcast_record(self, record_dict):
        """Sends JSON serialized record to all connected Spark socket clients."""
        payload = (json.dumps(record_dict) + "\n").encode("utf-8")
        with self.clients_lock:
            disconnected = []
            for client in self.clients:
                try:
                    client.sendall(payload)
                except Exception:
                    disconnected.append(client)
            for d in disconnected:
                self.clients.remove(d)

    def start_streaming(self):
        """Main loop that streams records from the dataset."""
        self.running = True
        self.start_socket_server()
        
        print(f"[PRODUCER] Loading ICU Telemetry dataset from {self.csv_path}...")
        try:
            # Read in chunks or read subset
            chunksize = 50000
            for chunk in pd.read_csv(self.csv_path, chunksize=chunksize):
                if not self.running:
                    break
                    
                for _, row in chunk.iterrows():
                    if not self.running:
                        break
                        
                    while self.paused and self.running:
                        time.sleep(0.2)
                        
                    record = {
                        "patient_id": int(row["Patient ID"]),
                        "heart_rate": float(row["Heart Rate"]),
                        "respiratory_rate": int(row["Respiratory Rate"]),
                        "timestamp": str(row["Timestamp"]),
                        "temperature": float(row["Body Temperature"]),
                        "oxygen_saturation": float(row["Oxygen Saturation"]),
                        "systolic_bp": int(row["Systolic Blood Pressure"]),
                        "diastolic_bp": int(row["Diastolic Blood Pressure"]),
                        "age": int(row["Age"]),
                        "gender": str(row["Gender"]),
                        "weight": float(row.get("Weight (kg)", 70.0)),
                        "height": float(row.get("Height (m)", 1.70)),
                        "hrv": float(row.get("Derived_HRV", 0.09)),
                        "pulse_pressure": int(row.get("Derived_Pulse_Pressure", 40)),
                        "bmi": float(row.get("Derived_BMI", 24.0)),
                        "map": float(row.get("Derived_MAP", 93.3)),
                        "risk_category": str(row.get("Risk Category", "Low Risk"))
                    }
                    
                    # 1. Broadcast to Spark via TCP socket
                    self.broadcast_record(record)
                    
                    # 2. Update bridge directly for smooth web dashboard rendering
                    bridge.update_patient_telemetry(record)
                    
                    time.sleep(self.delay_sec)
                    
        except Exception as e:
            print(f"[PRODUCER EXCEPTION] Stream loop error: {e}")
        finally:
            self.stop()

    def set_speed(self, multiplier):
        """Adjust streaming frequency: 1x, 2x, 5x, 10x."""
        base_delay = 0.5
        self.delay_sec = max(0.05, base_delay / max(0.1, multiplier))
        bridge.metrics["speed_multiplier"] = multiplier

    def pause(self):
        self.paused = True
        bridge.metrics["stream_active"] = False

    def resume(self):
        self.paused = False
        bridge.metrics["stream_active"] = True

    def stop(self):
        self.running = False
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass
        with self.clients_lock:
            for c in self.clients:
                try:
                    c.close()
                except Exception:
                    pass
            self.clients.clear()
        print("[PRODUCER] Telemetry stream stopped.")

# Singleton producer instance
producer = VitalSignsStreamProducer()

if __name__ == "__main__":
    print("=== Starting ICU Telemetry Wearable Stream Producer ===")
    try:
        producer.start_streaming()
    except KeyboardInterrupt:
        producer.stop()
        print("Producer terminated by user.")
