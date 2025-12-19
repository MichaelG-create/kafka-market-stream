"""
Kafka Producer - Market Indices Streaming
Reads market data from CSV and sends to Kafka topic 'market_indices_raw'
Using confluent-kafka library
"""

import csv
import json
import time
from confluent_kafka import Producer
from confluent_kafka import KafkaException

# Configuration
KAFKA_BROKER = "localhost:9092"
TOPIC_NAME = "market_indices_raw"
CSV_PATH = "data/indices_sample.csv"

def delivery_report(err, msg):
    """
    Callback for message delivery reports.
    Called once for each message produced to indicate delivery result.
    """
    if err is not None:
        print(f"❌ Message delivery failed: {err}")
    else:
        # Uncomment for verbose logging
        # print(f"✅ Message delivered to {msg.topic()} [{msg.partition()}] @ offset {msg.offset()}")
        pass

def create_producer():
    """Initialize Kafka producer"""
    conf = {
        'bootstrap.servers': KAFKA_BROKER,
        'client.id': 'market-indices-producer',
        'acks': 'all',  # Wait for all replicas
        'retries': 3,
        'max.in.flight.requests.per.connection': 1  # Ensure ordering
    }
    
    try:
        producer = Producer(conf)
        print(f"✅ Producer connected to Kafka at {KAFKA_BROKER}")
        return producer
    except KafkaException as e:
        print(f"❌ Failed to create producer: {e}")
        raise

def send_market_data(producer, csv_path, delay=0):
    """
    Read CSV and send each row as a JSON message to Kafka
    
    Args:
        producer: Producer instance
        csv_path: Path to CSV file
        delay: Delay in seconds between messages (for streaming simulation)
    """
    messages_sent = 0
    errors = 0
    
    print(f"📖 Reading data from {csv_path}...")
    
    try:
        with open(csv_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            
            for row in reader:
                # Build message payload
                message = {
                    "timestamp": row["timestamp"],
                    "symbol": row["symbol"],
                    "price": float(row["price"]),
                    "volume": int(row["volume"])
                }
                
                try:
                    # Serialize to JSON
                    message_json = json.dumps(message)
                    
                    # Send to Kafka (non-blocking)
                    producer.produce(
                        topic=TOPIC_NAME,
                        value=message_json.encode('utf-8'),
                        callback=delivery_report
                    )
                    
                    # Trigger delivery reports by polling
                    producer.poll(0)
                    
                    messages_sent += 1
                    
                    if messages_sent % 10 == 0:
                        print(f"✉️  Sent {messages_sent} messages (last: {message['symbol']} @ {message['price']})")
                    
                    # Simulate streaming with delay
                    if delay > 0:
                        time.sleep(delay)
                        
                except Exception as e:
                    print(f"❌ Error sending message: {e}")
                    errors += 1
                    
    except FileNotFoundError:
        print(f"❌ CSV file not found: {csv_path}")
        raise
    
    # Wait for all messages to be delivered
    print("\n⏳ Flushing remaining messages...")
    producer.flush()
    
    print(f"\n📊 Summary:")
    print(f"   Messages sent: {messages_sent}")
    print(f"   Errors: {errors}")
    
    return messages_sent, errors

def main():
    """Main execution"""
    print("🚀 Starting Kafka Producer - Market Indices Stream\n")
    
    # Create producer
    producer = create_producer()
    
    try:
        # Send data (no delay for now, we'll add it in US2-T4)
        send_market_data(producer, CSV_PATH, delay=0)
        
    except KeyboardInterrupt:
        print("\n⚠️  Interrupted by user")
    finally:
        print("✅ Producer closed")

if __name__ == "__main__":
    main()
