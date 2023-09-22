"""
Script to stream temperature data from a CSV file to RabbitMQ queues.
Intended to simulate a smart smoker system.

Author: Ivan Quackenbush
Date: 09/20/2023
"""

import csv
import pika
import time
import webbrowser

# RabbitMQ server settings
RABBIT_HOST = 'localhost'
RABBIT_PORT = 15672

QUEUE_NAMES = {
    '01-smoker': 'Channel1',
    '02-food-A': 'Channel2',
    '03-food-B': 'Channel3'
}

def open_rabbitmq_ui(host, port):
    """Launches RabbitMQ's UI in the default browser."""
    url = f"http://{host}:{port}/#/queues"
    webbrowser.open_new(url)
    print("RabbitMQ UI opened in default browser.")

def publish_temp_to_rabbitmq(timestamp, temperature, queue):
    """Publishes temperature data to a specified RabbitMQ queue."""
    try:
        conn = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        chan = conn.channel()
        chan.queue_declare(queue=queue, durable=True)

        msg = f"{timestamp}: {temperature}"
        chan.basic_publish(exchange='', routing_key=queue, body=msg,
                            properties=pika.BasicProperties(delivery_mode=2))  # Persistent message

        print(f"Sent: {msg} to Queue: '{queue}'")
        conn.close()
    except Exception as err:
        print(f"Failed to send temperature to '{queue}'. Error: {err}")

def main():
    """Main function to read and publish the temperature data."""
    print("Starting Smart Smoker Temperature Publisher...")

    # Open RabbitMQ UI
    open_rabbitmq_ui(RABBIT_HOST, RABBIT_PORT)

    csv_filename = 'smoker-temps.csv'
    sleep_time = 30  # in seconds

    try:
        with open(csv_filename, 'r') as file:
            csv_data = csv.DictReader(file)
            for row in csv_data:
                timestamp = row.get('Time (UTC)')
                
                for queue, channel in QUEUE_NAMES.items():
                    temp_str = row.get(channel, '').strip()
                    
                    if not temp_str:
                        print(f"Warning: No temperature data in '{channel}' column for timestamp {timestamp}.")
                        continue

                    try:
                        temperature = float(temp_str)
                        publish_temp_to_rabbitmq(timestamp, temperature, queue)
                    except ValueError:
                        print(f"Error: Invalid temperature in '{channel}' column for timestamp {timestamp}.")

                time.sleep(sleep_time)

    except KeyboardInterrupt:
        print("Stopping temperature publishing...")

if __name__ == '__main__':
    main()


