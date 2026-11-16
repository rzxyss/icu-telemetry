import os
import sys
import time
import json
import socket
import threading
import config

# Set Windows hadoop environment
if os.path.exists(config.HADOOP_HOME):
    os.environ["HADOOP_HOME"] = config.HADOOP_HOME
    os.environ["PATH"] = os.path.join(config.HADOOP_HOME, "bin") + os.pathsep + os.environ.get("PATH", "")

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, avg, count, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType, DoubleType
)
from streaming_bridge import bridge

def create_spark_session():
    """Initialize Apache Spark Session configured for Structured Streaming."""
    print("[SPARK] Initializing Apache Spark Structured Streaming Engine...")
    spark = (
        SparkSession.builder
        .appName("ICU_VitalSigns_StructuredStreaming")
        .master("local[*]")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.memory", "1g")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    print(f"[SPARK] Spark Engine Ready (Version {spark.version})")
    return spark

def get_telemetry_schema():
    """Define schema for incoming streaming vital signs JSON data."""
    return StructType([
        StructField("patient_id", IntegerType(), True),
        StructField("heart_rate", DoubleType(), True),
        StructField("respiratory_rate", IntegerType(), True),
        StructField("timestamp", StringType(), True),
        StructField("temperature", DoubleType(), True),
        StructField("oxygen_saturation", DoubleType(), True),
        StructField("systolic_bp", IntegerType(), True),
        StructField("diastolic_bp", IntegerType(), True),
        StructField("age", IntegerType(), True),
        StructField("gender", StringType(), True),
        StructField("weight", DoubleType(), True),
        StructField("height", DoubleType(), True),
        StructField("hrv", DoubleType(), True),
        StructField("pulse_pressure", IntegerType(), True),
        StructField("bmi", DoubleType(), True),
        StructField("map", DoubleType(), True),
        StructField("risk_category", StringType(), True),
    ])

def process_micro_batch(batch_df, batch_id):
    """
    ForeachBatch sink: Processes micro-batches from Spark Structured Streaming,
    performs clinical aggregation and updates metrics bridge.
    """
    start_time = time.time()
    record_count = batch_df.count()
    
    if record_count == 0:
        return
        
    # Micro-batch clinical aggregations
    metrics_row = batch_df.select(
        count("*").alias("total_count"),
        count(when(col("is_critical"), True)).alias("critical_count"),
        count(when(col("is_tachycardia"), True)).alias("tachycardia_count"),
        count(when(col("is_hypoxia"), True)).alias("hypoxia_count"),
        avg("heart_rate").alias("avg_hr"),
        avg("oxygen_saturation").alias("avg_spo2"),
        avg("map").alias("avg_map")
    ).collect()[0]
    
    latency_ms = (time.time() - start_time) * 1000.0
    
    # Update global streaming bridge
    bridge.record_spark_batch(batch_id, record_count, latency_ms)
    
    # Formatted console output for demonstration & audit
    print("\n" + "="*70)
    print(f" [SPARK MICRO-BATCH #{batch_id}] TIME: {time.strftime('%H:%M:%S')}")
    print(f" * Records Processed : {record_count} events")
    print(f" * Processing Latency: {latency_ms:.2f} ms")
    print(f" * Critical Alerts   : {metrics_row['critical_count']} events")
    print(f" * Tachycardia Cases : {metrics_row['tachycardia_count']}")
    print(f" * Hypoxia Cases     : {metrics_row['hypoxia_count']}")
    avg_hr = metrics_row['avg_hr'] or 0.0
    avg_spo2 = metrics_row['avg_spo2'] or 0.0
    avg_map = metrics_row['avg_map'] or 0.0
    print(f" * Batch Averages    : HR: {avg_hr:.1f} bpm | SpO2: {avg_spo2:.1f}% | MAP: {avg_map:.1f} mmHg")
    print(" * Fault-Tolerance   : Checkpoint State ACTIVE")
    print("="*70 + "\n")

def wait_for_socket_or_start_producer(host, port, max_wait=6):
    """
    Ensure the TCP socket producer is ready before starting Spark.
    If not yet running, starts stream_producer in a daemon thread.
    """
    print(f"[SPARK] Memeriksa ketersediaan socket producer di {host}:{port}...", flush=True)
    for _ in range(max_wait):
        try:
            with socket.create_connection((host, port), timeout=0.8):
                print(f"[SPARK] ✓ Terhubung ke socket producer aktif di {host}:{port}", flush=True)
                return True
        except (socket.error, ConnectionRefusedError):
            time.sleep(0.5)

    print(f"[SPARK] ℹ Port {port} belum terbuka. Menjalankan stream_producer otomatis...", flush=True)
    try:
        from stream_producer import producer
        t = threading.Thread(target=producer.start_streaming, daemon=True)
        t.start()
        time.sleep(1.5)
        print(f"[SPARK] ✓ Stream producer berhasil diaktifkan pada port {port}", flush=True)
        return True
    except Exception as e:
        print(f"[SPARK WARNING] Gagal mengaktifkan producer otomatis: {e}", flush=True)
        return False

def run_spark_streaming():
    """
    Main Spark Structured Streaming pipeline execution:
    Input Socket -> JSON Deserialization -> Anomaly Detection Transformations -> Micro-Batch ForeachBatch Sink
    """
    # Pastikan socket producer siap sebelum Spark readStream dipanggil
    wait_for_socket_or_start_producer(config.SOCKET_HOST, config.SOCKET_PORT)

    spark = create_spark_session()
    schema = get_telemetry_schema()
    
    print(f"[SPARK] Connecting stream input to TCP Socket {config.SOCKET_HOST}:{config.SOCKET_PORT}...")
    
    # 1. Input Source: Network Socket Stream (Module 8 Lab Task)
    raw_stream = (
        spark.readStream
        .format("socket")
        .option("host", config.SOCKET_HOST)
        .option("port", config.SOCKET_PORT)
        .load()
    )
    
    # 2. Deserialization & Schema Enforcement
    parsed_stream = (
        raw_stream
        .select(from_json(col("value"), schema).alias("data"))
        .select("data.*")
    )
    
    # 3. Real-Time Transformations: Clinical Rule & Pattern Detection
    transformed_stream = parsed_stream.withColumn(
        "is_tachycardia", col("heart_rate") > 100.0
    ).withColumn(
        "is_bradycardia", col("heart_rate") < 60.0
    ).withColumn(
        "is_hypoxia", col("oxygen_saturation") < 92.0
    ).withColumn(
        "is_hypertensive", (col("systolic_bp") >= 140) | (col("diastolic_bp") >= 90)
    ).withColumn(
        "is_hypoperfusion", col("map") < 65.0
    ).withColumn(
        "is_critical",
        (col("heart_rate") > 120.0) |
        (col("heart_rate") < 50.0) |
        (col("oxygen_saturation") < 90.0) |
        (col("systolic_bp") >= 160) |
        (col("map") < 65.0)
    )
    
    # Ensure checkpoint directory exists
    os.makedirs(config.CHECKPOINT_DIR, exist_ok=True)
    
    # 4. Output Sink: Micro-batch ForeachBatch with Checkpointing (Fault Tolerance)
    print(f"[SPARK] Starting Structured Streaming Query with Micro-Batching (Interval: {config.MICRO_BATCH_INTERVAL_SEC}s)...")
    query = (
        transformed_stream.writeStream
        .outputMode("append")
        .foreachBatch(process_micro_batch)
        .trigger(processingTime=f"{config.MICRO_BATCH_INTERVAL_SEC} seconds")
        .option("checkpointLocation", config.CHECKPOINT_DIR)
        .start()
    )
    
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("[SPARK] Stopping Streaming Query...")
        query.stop()
        spark.stop()
        print("[SPARK] Engine shutdown gracefully.")

if __name__ == "__main__":
    run_spark_streaming()
