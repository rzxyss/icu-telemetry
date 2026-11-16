import os
import sys
import json
import time
import threading
from flask import Flask, render_template, Response, jsonify, request
import config
from streaming_bridge import bridge
from stream_producer import producer

app = Flask(__name__, static_folder="static", template_folder="templates")
app.config["SECRET_KEY"] = "icu-telemetry-key-2024"

# Flag to track background streaming
producer_thread = None

def ensure_producer_running():
    """Ensure telemetry background producer is active."""
    global producer_thread
    if producer_thread is None or not producer.running:
        producer_thread = threading.Thread(target=producer.start_streaming, daemon=True)
        producer_thread.start()
        print("[FLASK] Telemetry stream producer started automatically in background thread.")

@app.route("/")
def index():
    """Render the ICU Telemetry Command Center Dashboard."""
    return render_template("index.html", 
                           title="ICU Telemetry Command Center",
                           max_beds=config.MAX_ACTIVE_BEDS)

@app.route("/api/stream")
def sse_stream():
    """
    Server-Sent Events (SSE) endpoint providing low-latency,
    real-time continuous telemetry push to client browsers.
    """
    def event_generator():
        q = bridge.register_listener()
        try:
            # Send initial state snapshot on connect
            init_data = json.dumps({
                "event_type": "initial_state",
                "beds": bridge.get_all_beds(),
                "metrics": bridge.get_metrics(),
                "alerts": bridge.get_recent_alerts(25)
            })
            yield f"data: {init_data}\n\n"
            
            while True:
                if len(q) > 0:
                    payload = q.popleft()
                    yield f"data: {payload}\n\n"
                else:
                    time.sleep(0.1)
        except GeneratorExit:
            bridge.unregister_listener(q)
        except Exception as e:
            bridge.unregister_listener(q)

    return Response(event_generator(), mimetype="text/event-stream", headers={
        "Cache-Control": "no-cache",
        "Connection": "keep-alive",
        "X-Accel-Buffering": "no"
    })

@app.route("/api/status")
def get_status():
    """Return streaming engine and micro-batch statistics."""
    return jsonify(bridge.get_metrics())

@app.route("/api/beds")
def get_beds():
    """Return latest vital signs and status of all ICU beds."""
    return jsonify({
        "success": True,
        "count": len(bridge.beds),
        "beds": bridge.get_all_beds()
    })

@app.route("/api/bed/<int:bed_id>")
def get_bed_detail(bed_id):
    """Return detailed telemetry history for a specific bed."""
    bed = bridge.get_bed(bed_id)
    if not bed:
        return jsonify({"success": False, "error": "Bed not found"}), 404
    return jsonify({"success": True, "bed": bed})

@app.route("/api/alerts")
def get_alerts():
    """Return recent clinical triage alerts."""
    limit = request.args.get("limit", default=30, type=int)
    return jsonify({
        "success": True,
        "alerts": bridge.get_recent_alerts(limit)
    })

@app.route("/api/control/stream", methods=["POST"])
def control_stream():
    """Toggle play / pause of patient telemetry stream."""
    data = request.get_json() or {}
    action = data.get("action", "toggle")
    
    if action == "pause":
        producer.pause()
    elif action == "resume":
        producer.resume()
    else:
        if producer.paused:
            producer.resume()
        else:
            producer.pause()
            
    return jsonify({
        "success": True,
        "stream_active": not producer.paused,
        "metrics": bridge.get_metrics()
    })

@app.route("/api/control/speed", methods=["POST"])
def control_speed():
    """Adjust stream playback speed multiplier (1x, 2x, 5x, 10x)."""
    data = request.get_json() or {}
    multiplier = float(data.get("multiplier", 1.0))
    producer.set_speed(multiplier)
    return jsonify({
        "success": True,
        "speed_multiplier": multiplier,
        "metrics": bridge.get_metrics()
    })

@app.route("/api/control/inject_anomaly", methods=["POST"])
def inject_anomaly():
    """
    Simulate instant clinical emergency anomaly (Tachycardia, Hypoxia, Crisis)
    to demonstrate Spark Structured Streaming real-time detection.
    """
    data = request.get_json() or {}
    anomaly_type = data.get("type", "TACHYCARDIA")
    target_bed = data.get("bed_id", None)
    
    injected = bridge.inject_anomaly(anomaly_type, target_bed)
    return jsonify({
        "success": True,
        "injected": injected,
        "message": f"Clinical emergency '{anomaly_type}' queued for real-time detection!"
    })

if __name__ == "__main__":
    ensure_producer_running()
    print(f"\n========================================================")
    print(f" [ICU COMMAND CENTER] Dashboard running at: http://{config.FLASK_HOST}:{config.FLASK_PORT}")
    print(f"========================================================\n")
    app.run(host=config.FLASK_HOST, port=config.FLASK_PORT, debug=True, threaded=True)
