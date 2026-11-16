import os

BASE_DIR = os.path.abspath(os.path.dirname(__file__))
DATASET_PATH = os.path.join(BASE_DIR, "dataset", "human_vital_signs_dataset_2024.csv")
CHECKPOINT_DIR = os.path.join(BASE_DIR, "checkpoints", "icu_vital_stream")
HADOOP_HOME = os.path.join(BASE_DIR, "hadoop")

# Set HADOOP_HOME for Windows PySpark
if os.path.exists(HADOOP_HOME):
    os.environ["HADOOP_HOME"] = HADOOP_HOME
    os.environ["PATH"] = os.path.join(HADOOP_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

# Networking Configuration
SOCKET_HOST = "127.0.0.1"
SOCKET_PORT = 9999
FLASK_HOST = "127.0.0.1"
FLASK_PORT = 5000

# Streaming & Micro-Batching Configuration
MICRO_BATCH_INTERVAL_SEC = 2
STREAM_SPEED_MULTIPLIER = 1.0  # Speed factor for playback
MAX_ACTIVE_BEDS = 12           # Visualized active ICU beds in dashboard

# Clinical Thresholds (ICU Telemetry Standards)
THRESHOLDS = {
    "heart_rate": {
        "bradycardia_critical": 50,
        "bradycardia_warning": 60,
        "normal_min": 60,
        "normal_max": 100,
        "tachycardia_warning": 100,
        "tachycardia_critical": 125,
    },
    "oxygen_saturation": {
        "critical_low": 90.0,
        "warning_low": 94.0,
        "normal_min": 95.0,
    },
    "blood_pressure": {
        "systolic_crisis": 140,
        "diastolic_crisis": 90,
        "systolic_severe": 160,
    },
    "respiratory_rate": {
        "bradypnea": 10,
        "tachypnea": 22,
    },
    "temperature": {
        "hypothermia": 35.5,
        "fever": 38.0,
    },
    "map": {
        "hypoperfusion": 65.0,
        "hypertensive": 105.0,
    }
}
