from kafka import KafkaProducer
from flask import Flask, send_from_directory, jsonify, request
from flask_socketio import SocketIO, emit
import json
import threading
from collections import defaultdict
from datetime import datetime
import uuid

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*")

# Kafka producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    max_request_size=10485760  # 10MB
)

# Store Spark results in memory for quick access
spark_analytics = defaultdict(lambda: {
    "avg_wpm": 0,
    "avg_accuracy": 0,
    "total_keystrokes": 0,
    "global_rank": "N/A",
    "performance_trend": "stable",
    "last_update": None
})

# Store real-time leaderboard
leaderboard = []
typing_sessions = {}

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/<path:path>')
def serve_file(path):
    return send_from_directory('.', path)

@app.route('/api/stats/<user_id>')
def get_user_stats(user_id):
    """Get Spark-analyzed statistics for a user"""
    stats = spark_analytics.get(user_id, {
        "avg_wpm": 0,
        "avg_accuracy": 0,
        "total_keystrokes": 0,
        "global_rank": "N/A",
        "performance_trend": "stable"
    })
    return jsonify(stats)

@app.route('/api/leaderboard')
def get_leaderboard():
    """Get top performers from Spark analysis"""
    # Sort by WPM and return top 10
    sorted_leaderboard = sorted(
        [{"username": k, "avg_wpm": v["avg_wpm"], "avg_accuracy": v["avg_accuracy"]} 
         for k, v in spark_analytics.items() if v["avg_wpm"] > 0],
        key=lambda x: x["avg_wpm"],
        reverse=True
    )[:10]
    return jsonify(sorted_leaderboard)

@app.route('/api/spark_result', methods=['POST'])
def receive_spark_result():
    """Endpoint for Spark to send processed results back"""
    try:
        data = request.json
        user_id = data.get('userId')
        
        if user_id:
            spark_analytics[user_id] = {
                "avg_wpm": round(data.get('avg_wpm', 0), 1),
                "avg_accuracy": round(data.get('avg_accuracy', 0), 1),
                "total_keystrokes": data.get('keystroke_count', 0),
                "global_rank": data.get('global_rank', 'N/A'),
                "performance_trend": data.get('performance_trend', 'stable'),
                "last_update": datetime.now().isoformat()
            }
            
            # Broadcast to all connected clients
            socketio.emit('spark_update', {
                'userId': user_id,
                'stats': spark_analytics[user_id]
            })
            
            print(f"✅ Received Spark results for {user_id}: {spark_analytics[user_id]}")
            
    except Exception as e:
        print(f"Error receiving Spark result: {e}")
    
    return "OK", 200

@app.route('/api/health')
def health_check():
    """Health check endpoint"""
    return jsonify({
        "status": "healthy",
        "kafka_connected": producer.bootstrap_connected(),
        "active_sessions": len(typing_sessions),
        "spark_analytics_count": len(spark_analytics)
    })

# Add this endpoint to your kafka_bridge.py, right after the other @app.route methods

@app.route('/api/leaderboard_update', methods=['POST'])
def update_leaderboard():
    """Receive leaderboard updates from Spark"""
    try:
        data = request.json
        # Store in global leaderboard variable
        global leaderboard
        leaderboard = data.get('leaderboard', [])
        # Broadcast to all clients
        socketio.emit('leaderboard_update', leaderboard)
        return "OK", 200
    except Exception as e:
        print(f"Error updating leaderboard: {e}")
        return "Error", 500

@socketio.on('typing_event')
def handle_typing_event(data):
    """Receive typing events from website and send to Kafka"""
    try:
        # Add unique event ID
        data['event_id'] = str(uuid.uuid4())
        data['server_timestamp'] = datetime.now().isoformat()
        
        print(f"📨 Received: {data['eventType']} from {data.get('userId', 'unknown')}")
        
        # Send to Kafka for Spark processing
        future = producer.send('typing-events', value=data)
        
        # Optional: Wait for acknowledgment
        metadata = future.get(timeout=5)
        print(f"   ✅ Sent to Kafka partition {metadata.partition} offset {metadata.offset}")
        
        # Track session start/end
        if data['eventType'] == 'SESSION_START':
            typing_sessions[data['userId']] = {
                'start_time': datetime.now(),
                'events_count': 0
            }
        elif data['eventType'] == 'TEST_COMPLETE':
            if data['userId'] in typing_sessions:
                typing_sessions[data['userId']]['end_time'] = datetime.now()
        
        if data['userId'] in typing_sessions:
            typing_sessions[data['userId']]['events_count'] += 1
            
    except Exception as e:
        print(f"❌ Error sending to Kafka: {e}")
        emit('kafka_error', {'message': str(e)})

@socketio.on('get_analytics')
def send_analytics(data):
    """Send current analytics to specific user"""
    user_id = data.get('userId')
    if user_id and user_id in spark_analytics:
        emit('spark_update', {
            'userId': user_id,
            'stats': spark_analytics[user_id]
        })

if __name__ == '__main__':
    print("=" * 60)
    print("🚀 KAFKA BRIDGE WITH SPARK INTEGRATION")
    print("=" * 60)
    print(f"📍 Server: http://localhost:5000")
    print(f"📡 Kafka: localhost:9092")
    print(f"📊 Spark Analytics Endpoint: /api/spark_result")
    print(f"🏆 Leaderboard: /api/leaderboard")
    print("=" * 60)
    print("\n✅ Ready to accept connections...\n")
    
    socketio.run(app, host='0.0.0.0', port=5000, debug=True, allow_unsafe_werkzeug=True)