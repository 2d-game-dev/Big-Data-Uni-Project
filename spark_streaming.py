from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, avg, count, stddev, when, lit, struct, to_json
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
import requests
from datetime import datetime

print("=" * 60)
print("🔥 SPARK STREAMING ANALYTICS ENGINE")
print("=" * 60)

# Create Spark session
spark = SparkSession.builder \
    .appName("TypingTestAnalytics") \
    .master("local[2]") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("✅ Spark Session Created Successfully!")

# Define the schema for incoming data
data_schema = StructType([
    StructField("accuracy", IntegerType(), True),
    StructField("wpm", IntegerType(), True),
    StructField("correct", IntegerType(), True),
    StructField("total", IntegerType(), True),
    StructField("finalWPM", IntegerType(), True),
    StructField("finalAccuracy", IntegerType(), True),
    StructField("quote", StringType(), True),
    StructField("timestamp", DoubleType(), True)
])

schema = StructType([
    StructField("eventType", StringType(), True),
    StructField("userId", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("event_id", StringType(), True),
    StructField("server_timestamp", StringType(), True),
    StructField("data", data_schema, True)
])

print("✅ Schema defined")

def send_to_webhook(batch_df, batch_id):
    """Send Spark processed results back to Flask bridge"""
    if batch_df.count() == 0:
        return
    
    try:
        # Calculate global statistics
        global_avg_wpm = batch_df.agg(avg("avg_wpm")).collect()[0][0] or 0
        global_stddev = batch_df.agg(stddev("avg_wpm")).collect()[0][0] or 10
        
        # Process each user
        for row in batch_df.collect():
            user_id = row.userId
            
            # Determine rank
            if global_avg_wpm > 0:
                if row.avg_wpm > global_avg_wpm + global_stddev:
                    global_rank = "🏆 TOP 10%"
                elif row.avg_wpm > global_avg_wpm:
                    global_rank = "📈 ABOVE AVERAGE"
                elif row.avg_wpm > global_avg_wpm - global_stddev:
                    global_rank = "📊 AVERAGE"
                else:
                    global_rank = "🎯 NEEDS PRACTICE"
            else:
                global_rank = "⭐ FIRST TEST"
            
            # Performance trend based on WPM
            if row.avg_wpm >= 50:
                performance_trend = "🚀 FAST TYPIST"
            elif row.avg_wpm >= 30:
                performance_trend = "👍 MODERATE"
            else:
                performance_trend = "📚 LEARNING"
            
            # Send to Flask
            result_data = {
                "userId": user_id,
                "avg_wpm": round(float(row.avg_wpm), 1),
                "avg_accuracy": round(float(row.avg_accuracy), 1),
                "keystroke_count": int(row.keystroke_count),
                "global_rank": global_rank,
                "performance_trend": performance_trend,
                "timestamp": datetime.now().isoformat()
            }
            
            try:
                response = requests.post(
                    'http://localhost:5000/api/spark_result',
                    json=result_data,
                    timeout=2
                )
                if response.status_code == 200:
                    print(f"📊 {user_id}: {result_data['avg_wpm']} WPM ({global_rank})")
            except Exception as e:
                print(f"⚠️ Could not send: {e}")
                
    except Exception as e:
        print(f"❌ Error: {e}")

try:
    # Read from Kafka
    print("📡 Connecting to Kafka...")
    df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "typing-events") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .option("maxOffsetsPerTrigger", "50") \
        .load() \
        .selectExpr("CAST(value AS STRING) as json") \
        .select(from_json(col("json"), schema).alias("data")) \
        .select("data.*")
    
    print("✅ Connected to Kafka stream")
    
    # Filter keystroke events that have WPM data
    keystrokes = df.filter(
        (col("eventType") == "KEYSTROKE") & 
        col("data.wpm").isNotNull()
    )
    
    # Calculate statistics per user (simplified - no window functions)
    user_stats = keystrokes.groupBy("userId").agg(
        avg("data.wpm").alias("avg_wpm"),
        avg("data.accuracy").alias("avg_accuracy"),
        count("*").alias("keystroke_count"),
        stddev("data.wpm").alias("wpm_variance")
    ).filter(col("keystroke_count") > 2)  # Need at least 3 keystrokes
    
    # Add performance labels
    user_stats_with_labels = user_stats.withColumn(
        "performance_level",
        when(col("avg_wpm") >= 60, "🏆 EXPERT")
        .when(col("avg_wpm") >= 40, "👍 INTERMEDIATE")
        .when(col("avg_wpm") >= 20, "📚 BEGINNER")
        .otherwise("🎯 PRACTICE NEEDED")
    )
    
    print("✅ Analytics pipeline defined")
    
    # Write to console (for professor to see)
    console_query = user_stats_with_labels.writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .trigger(processingTime="10 seconds") \
        .start()
    
    # Send to Flask bridge
    flask_query = user_stats_with_labels.writeStream \
        .outputMode("complete") \
        .foreachBatch(send_to_webhook) \
        .trigger(processingTime="15 seconds") \
        .start()
    
    print("=" * 60)
    print("✅ SPARK STREAMING IS RUNNING!")
    print("📊 Analytics every 10 seconds")
    print("🔄 Results sent to Flask bridge")
    print("=" * 60)
    print("\n💡 Start typing on the website!")
    print("   After 10-15 seconds, you'll see:\n")
    print("   - Console output below")
    print("   - Spark Analytics card on website")
    print("   - Global Leaderboard\n")
    
    console_query.awaitTermination()
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print("\nMake sure Kafka is running and topic 'typing-events' exists")

print("Spark application ended")