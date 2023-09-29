import pika
from collections import deque

# RabbitMQ configurations
rabbit_host = 'localhost'
rabbit_port = 5672  # Default RabbitMQ communication port

# Deque for storing smoker temperature readings
smoker_temps = deque(maxlen=20)

def smoker_callback(ch, method, properties, body):
    # Extract temperature and timestamp from the message
    decoded_body = body.decode('utf-8')
    timestamp, temp = decoded_body.split(": ")
    temp_float = float(temp)
    
    # Append to the deque
    smoker_temps.append(temp_float)
    
    # Check for unusual temperature fluctuation in the smoker
    if len(smoker_temps) == 20 and (max(smoker_temps) - min(smoker_temps)) >= 15:
        print(f"Smoker Alert at {timestamp}: Temperature fluctuated by 15°F or more in the last 10 minutes!")
    
    # Print the current temperature
    print(f"Smoker Temperature at {timestamp}: {temp}°F")

# Connect to RabbitMQ and start consuming messages
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port))
channel = connection.channel()

# Ensure queue exists
channel.queue_declare(queue='01-smoker', durable=True)

# Start consuming
channel.basic_consume(queue='01-smoker', on_message_callback=smoker_callback, auto_ack=True)
print("Smoker Monitor is waiting for messages. To exit, press Ctrl+C")
channel.start_consuming()
