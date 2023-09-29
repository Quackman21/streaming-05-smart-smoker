import pika
from collections import deque

# RabbitMQ configurations
rabbit_host = 'localhost'
rabbit_port = 5672  # Default RabbitMQ communication port

# Deque for storing Food B temperature readings
food_b_temps = deque(maxlen=20)

def food_b_callback(ch, method, properties, body):
    # Extract temperature and timestamp from the message
    decoded_body = body.decode('utf-8')
    timestamp, temp = decoded_body.split(": ")
    temp_float = float(temp)
    
    # Append to the deque
    food_b_temps.append(temp_float)
    
    # Check for food stall condition
    if len(food_b_temps) == 20 and abs(food_b_temps[-1] - food_b_temps[0]) <= 1:
        print(f"Food B Stall Alert at {timestamp}: Temperature changed by 1°F or less in 10 minutes!")
    
    # Print the current temperature
    print(f"Food B Temperature at {timestamp}: {temp}°F")

# Connect to RabbitMQ and start consuming messages
connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbit_host, port=rabbit_port))
channel = connection.channel()

# Ensure queue exists
channel.queue_declare(queue='03-food-B', durable=True)

# Start consuming
channel.basic_consume(queue='03-food-B', on_message_callback=food_b_callback, auto_ack=True)
print("Food B Consumer is waiting for messages. To exit, press Ctrl+C")
channel.start_consuming()
