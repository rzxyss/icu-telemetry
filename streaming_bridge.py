import time
import json
import threading
from collections import deque
from datetime import datetime
import config

class VitalStreamBridge:
    """
    Central thread-safe bridge connecting the Spark Streaming processing
    engine with Flask real-time Server-Sent Events (SSE) and REST endpoints.
    """
    def __init__(self):
        self.lock = threading.Lock()
        self.listeners = []
        
        # Micro-batch streaming metrics
        self.metrics = {
            "batch_id": 0,
            "total_records_processed": 0,
            "current_batch_count": 0,
            "processing_latency_ms": 0.0,
            "input_rate_records_sec": 0.0,
            "spark_active": False,
            "stream_active": True,
            "speed_multiplier": 1.0,
            "checkpoint_status": "SYNCD",
            "last_updated": datetime.now().isoformat(),
            "critical_alerts_count": 0,
            "warning_alerts_count": 0,
            "normal_count": 0
        }
        
        # Bed registry (e.g. Bed 01 to Bed 12)
        self.beds = {}
        for bed_id in range(1, config.MAX_ACTIVE_BEDS + 1):
            self.beds[bed_id] = {
                "bed_id": bed_id,
                "bed_label": f"ICU-BED-{bed_id:02d}",
                "patient_id": None,
                "age": 0,
                "gender": "-",
                "status": "AWAITING_TELEMETRY",
                "risk_category": "Normal",
                "risk_color": "emerald",
                "vitals": {
                    "heart_rate": 75,
                    "oxygen_saturation": 98.0,
                    "systolic_bp": 120,
                    "diastolic_bp": 80,
                    "respiratory_rate": 16,
                    "temperature": 36.8,
                    "map": 93.3,
                    "hrv": 0.10,
                    "bmi": 24.5
                },
                "active_alerts": [],
                "last_timestamp": None,
                "history": {
                    "timestamps": deque(maxlen=30),
                    "heart_rate": deque(maxlen=30),
                    "oxygen_saturation": deque(maxlen=30),
                    "systolic_bp": deque(maxlen=30),
                    "diastolic_bp": deque(maxlen=30),
                    "map": deque(maxlen=30)
                }
            }
            
        # Recent clinical alert log (FIFO queue of latest 100 alerts)
        self.alert_feed = deque(maxlen=100)
        
        # Windowed aggregation history for charting
        self.window_stats = deque(maxlen=20)
        
        # Simulated manual anomalies queue
        self.injected_anomalies = []

    def evaluate_clinical_status(self, vitals):
        """
        Rule-based medical telemetry evaluation matching clinical ICU guidelines.
        """
        hr = vitals.get("heart_rate", 80)
        spo2 = vitals.get("oxygen_saturation", 98.0)
        sys_bp = vitals.get("systolic_bp", 120)
        dia_bp = vitals.get("diastolic_bp", 80)
        resp = vitals.get("respiratory_rate", 16)
        temp = vitals.get("temperature", 36.8)
        map_val = vitals.get("map", (2 * dia_bp + sys_bp) / 3.0)
        
        alerts = []
        severity = "NORMAL"  # NORMAL, WARNING, CRITICAL
        
        # Oxygen Saturation (SpO2)
        if spo2 < config.THRESHOLDS["oxygen_saturation"]["critical_low"]:
            alerts.append({"type": "HYPOXIA_CRITICAL", "message": f"Desaturasi Oksigen Kritis ({spo2:.1f}%)", "level": "CRITICAL"})
            severity = "CRITICAL"
        elif spo2 < config.THRESHOLDS["oxygen_saturation"]["warning_low"]:
            alerts.append({"type": "HYPOXIA_WARNING", "message": f"Saturasi Rendah ({spo2:.1f}%)", "level": "WARNING"})
            if severity != "CRITICAL":
                severity = "WARNING"
                
        # Heart Rate
        if hr > config.THRESHOLDS["heart_rate"]["tachycardia_critical"]:
            alerts.append({"type": "TACHYCARDIA_CRITICAL", "message": f"Takikardia Akut ({hr} bpm)", "level": "CRITICAL"})
            severity = "CRITICAL"
        elif hr > config.THRESHOLDS["heart_rate"]["tachycardia_warning"]:
            alerts.append({"type": "TACHYCARDIA_WARNING", "message": f"Takikardia ({hr} bpm)", "level": "WARNING"})
            if severity != "CRITICAL":
                severity = "WARNING"
        elif hr < config.THRESHOLDS["heart_rate"]["bradycardia_critical"]:
            alerts.append({"type": "BRADYCARDIA_CRITICAL", "message": f"Bradikardia Kritis ({hr} bpm)", "level": "CRITICAL"})
            severity = "CRITICAL"
        elif hr < config.THRESHOLDS["heart_rate"]["bradycardia_warning"]:
            alerts.append({"type": "BRADYCARDIA_WARNING", "message": f"Bradikardia ({hr} bpm)", "level": "WARNING"})
            if severity != "CRITICAL":
                severity = "WARNING"
                
        # Blood Pressure
        if sys_bp >= config.THRESHOLDS["blood_pressure"]["systolic_severe"] or dia_bp >= 110:
            alerts.append({"type": "HYPERTENSION_CRISIS", "message": f"Krisis Hipertensi Parah ({sys_bp}/{dia_bp} mmHg)", "level": "CRITICAL"})
            severity = "CRITICAL"
        elif sys_bp >= config.THRESHOLDS["blood_pressure"]["systolic_crisis"] or dia_bp >= config.THRESHOLDS["blood_pressure"]["diastolic_crisis"]:
            alerts.append({"type": "HYPERTENSION_STAGE2", "message": f"Hipertensi Signifikan ({sys_bp}/{dia_bp} mmHg)", "level": "WARNING"})
            if severity != "CRITICAL":
                severity = "WARNING"
                
        # MAP (Mean Arterial Pressure)
        if map_val < config.THRESHOLDS["map"]["hypoperfusion"]:
            alerts.append({"type": "HYPOPERFUSION", "message": f"MAP Rendah ({map_val:.1f} mmHg) - Risiko Hipoperfusi", "level": "CRITICAL"})
            severity = "CRITICAL"
            
        # Temperature
        if temp >= config.THRESHOLDS["temperature"]["fever"]:
            alerts.append({"type": "FEVER_HIGH", "message": f"Hipertermia / Demam Tinggi ({temp:.1f}°C)", "level": "WARNING"})
            if severity != "CRITICAL":
                severity = "WARNING"
        elif temp <= config.THRESHOLDS["temperature"]["hypothermia"]:
            alerts.append({"type": "HYPOTHERMIA", "message": f"Hipotermia ({temp:.1f}°C)", "level": "WARNING"})
            if severity != "CRITICAL":
                severity = "WARNING"

        # Determine color
        if severity == "CRITICAL":
            color = "rose"
            status = "CRITICAL DETERIORATION"
        elif severity == "WARNING":
            color = "amber"
            status = "PATIENT WARNING"
        else:
            color = "emerald"
            status = "STABLE"
            
        return severity, status, color, alerts

    def update_patient_telemetry(self, raw_record, batch_id=None):
        """
        Process a single streaming vital record and update bed state.
        """
        with self.lock:
            # Map Patient ID to Bed 1..12
            patient_id = int(raw_record.get("patient_id", 1))
            bed_id = ((patient_id - 1) % config.MAX_ACTIVE_BEDS) + 1
            
            bed = self.beds[bed_id]
            bed["patient_id"] = patient_id
            bed["age"] = int(raw_record.get("age", 45))
            bed["gender"] = str(raw_record.get("gender", "Unknown"))
            
            # Check if there is an active injected anomaly for this bed or any bed
            injected = self._pop_matching_anomaly(bed_id)
            
            hr = float(raw_record.get("heart_rate", 75))
            spo2 = float(raw_record.get("oxygen_saturation", 98.0))
            sys_bp = int(raw_record.get("systolic_bp", 120))
            dia_bp = int(raw_record.get("diastolic_bp", 80))
            resp = int(raw_record.get("respiratory_rate", 16))
            temp = float(raw_record.get("temperature", 36.8))
            map_val = float(raw_record.get("map", (2 * dia_bp + sys_bp) / 3.0))
            hrv = float(raw_record.get("hrv", 0.08))
            bmi = float(raw_record.get("bmi", 23.5))
            
            # Apply injected anomaly if exists
            if injected:
                if injected["type"] == "TACHYCARDIA":
                    hr = 145.0
                    hrv = 0.02
                elif injected["type"] == "HYPOXIA":
                    spo2 = 86.5
                    resp = 28
                elif injected["type"] == "HYPERTENSION":
                    sys_bp = 185
                    dia_bp = 115
                    map_val = (2 * dia_bp + sys_bp) / 3.0
                elif injected["type"] == "BRADYCARDIA":
                    hr = 42.0
                    
            vitals = {
                "heart_rate": round(hr, 1),
                "oxygen_saturation": round(spo2, 1),
                "systolic_bp": sys_bp,
                "diastolic_bp": dia_bp,
                "respiratory_rate": resp,
                "temperature": round(temp, 1),
                "map": round(map_val, 1),
                "hrv": round(hrv, 3),
                "bmi": round(bmi, 1)
            }
            
            severity, status, color, alerts = self.evaluate_clinical_status(vitals)
            
            # If dataset labeled High Risk and no other alert, flag as clinical surveillance
            raw_risk = raw_record.get("risk_category", "Low Risk")
            if raw_risk == "High Risk" and severity == "NORMAL":
                severity = "WARNING"
                color = "amber"
                status = "ELEVATED RISK"
                alerts.append({"type": "HIGH_RISK_SURVEILLANCE", "message": "High Risk Category Marker", "level": "WARNING"})
            
            timestamp_str = raw_record.get("timestamp", datetime.now().strftime("%H:%M:%S"))
            if " " in timestamp_str:
                display_time = timestamp_str.split(" ")[1][:8]
            else:
                display_time = timestamp_str[:8]
                
            bed["vitals"] = vitals
            bed["status"] = status
            bed["risk_category"] = severity
            bed["risk_color"] = color
            bed["active_alerts"] = alerts
            bed["last_timestamp"] = display_time
            
            # Update history deques
            bed["history"]["timestamps"].append(display_time)
            bed["history"]["heart_rate"].append(vitals["heart_rate"])
            bed["history"]["oxygen_saturation"].append(vitals["oxygen_saturation"])
            bed["history"]["systolic_bp"].append(vitals["systolic_bp"])
            bed["history"]["diastolic_bp"].append(vitals["diastolic_bp"])
            bed["history"]["map"].append(vitals["map"])
            
            # Record alert in feed if critical or warning
            for al in alerts:
                alert_entry = {
                    "id": f"{int(time.time()*1000)}-{bed_id}",
                    "bed_id": bed_id,
                    "bed_label": bed["bed_label"],
                    "patient_id": patient_id,
                    "timestamp": display_time,
                    "level": al["level"],
                    "type": al["type"],
                    "message": al["message"],
                    "vitals_snapshot": f"HR: {vitals['heart_rate']} | SpO2: {vitals['oxygen_saturation']}% | BP: {sys_bp}/{dia_bp}"
                }
                self.alert_feed.appendleft(alert_entry)
                
            # Recalculate summary metrics
            self._update_summary_counts()
            
            # Broadcast to SSE listeners
            update_payload = {
                "event_type": "bed_update",
                "bed": self._serialize_bed(bed),
                "metrics": self.metrics,
                "latest_alert": self.alert_feed[0] if self.alert_feed else None
            }
            self._broadcast(update_payload)
            
            return bed

    def record_spark_batch(self, batch_id, record_count, latency_ms):
        """
        Record metadata from a completed Spark Structured Streaming micro-batch.
        """
        with self.lock:
            self.metrics["batch_id"] = batch_id
            self.metrics["total_records_processed"] += record_count
            self.metrics["current_batch_count"] = record_count
            self.metrics["processing_latency_ms"] = round(latency_ms, 2)
            if latency_ms > 0:
                self.metrics["input_rate_records_sec"] = round((record_count / (latency_ms / 1000.0)), 1)
            self.metrics["spark_active"] = True
            self.metrics["checkpoint_status"] = "SYNCD"
            self.metrics["last_updated"] = datetime.now().isoformat()
            
            # Compute window stats
            now_str = datetime.now().strftime("%H:%M:%S")
            self.window_stats.append({
                "time": now_str,
                "batch_id": batch_id,
                "records": record_count,
                "latency_ms": round(latency_ms, 1)
            })
            
            batch_payload = {
                "event_type": "spark_batch_completed",
                "metrics": self.metrics
            }
            self._broadcast(batch_payload)

    def inject_anomaly(self, anomaly_type="TACHYCARDIA", target_bed=None):
        """
        Allows interactive injection of clinical emergencies during presentation.
        """
        with self.lock:
            anomaly = {
                "type": anomaly_type.upper(),
                "target_bed": int(target_bed) if target_bed else None,
                "created_at": time.time()
            }
            self.injected_anomalies.append(anomaly)
            return anomaly

    def _pop_matching_anomaly(self, bed_id):
        for idx, an in enumerate(self.injected_anomalies):
            if an["target_bed"] is None or an["target_bed"] == bed_id:
                return self.injected_anomalies.pop(idx)
        return None

    def _update_summary_counts(self):
        crit = 0
        warn = 0
        norm = 0
        for b in self.beds.values():
            if b["risk_category"] == "CRITICAL":
                crit += 1
            elif b["risk_category"] == "WARNING":
                warn += 1
            else:
                norm += 1
        self.metrics["critical_alerts_count"] = crit
        self.metrics["warning_alerts_count"] = warn
        self.metrics["normal_count"] = norm

    def _serialize_bed(self, bed):
        return {
            "bed_id": bed["bed_id"],
            "bed_label": bed["bed_label"],
            "patient_id": bed["patient_id"],
            "age": bed["age"],
            "gender": bed["gender"],
            "status": bed["status"],
            "risk_category": bed["risk_category"],
            "risk_color": bed["risk_color"],
            "vitals": bed["vitals"],
            "active_alerts": bed["active_alerts"],
            "last_timestamp": bed["last_timestamp"],
            "history": {
                "timestamps": list(bed["history"]["timestamps"]),
                "heart_rate": list(bed["history"]["heart_rate"]),
                "oxygen_saturation": list(bed["history"]["oxygen_saturation"]),
                "systolic_bp": list(bed["history"]["systolic_bp"]),
                "diastolic_bp": list(bed["history"]["diastolic_bp"]),
                "map": list(bed["history"]["map"])
            }
        }

    def get_all_beds(self):
        with self.lock:
            return [self._serialize_bed(b) for b in self.beds.values()]

    def get_bed(self, bed_id):
        with self.lock:
            if bed_id in self.beds:
                return self._serialize_bed(self.beds[bed_id])
            return None

    def get_recent_alerts(self, limit=50):
        with self.lock:
            return list(self.alert_feed)[:limit]

    def get_metrics(self):
        with self.lock:
            return dict(self.metrics)

    def register_listener(self):
        q = deque(maxlen=20)
        with self.lock:
            self.listeners.append(q)
        return q

    def unregister_listener(self, q):
        with self.lock:
            if q in self.listeners:
                self.listeners.remove(q)

    def _broadcast(self, payload):
        # Notify subscribers
        json_str = json.dumps(payload)
        for q in list(self.listeners):
            try:
                q.append(json_str)
            except Exception:
                pass

# Global Singleton Bridge
bridge = VitalStreamBridge()
