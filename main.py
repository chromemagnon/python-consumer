
import logging
import os

from service.consumer_service import ConsumerService

# Configurations
LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO').upper()

# Setup logging
logging.basicConfig(level=LOG_LEVEL)

def main():
    """
    Main function to initialize and start the consumer service.
    """
    consumer = ConsumerService()
    try:
        consumer.setup_connection()
        consumer.start_consuming()
    except Exception as e:
        logging.error(f"Error occurred: {e}")
    finally:
        logging.info("Stopping consumer")
        consumer.close_connection()


if __name__ == '__main__':
    main()

