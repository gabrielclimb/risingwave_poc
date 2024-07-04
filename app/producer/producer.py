import argparse
import json
import random
import sys
import time

from faker import Faker
from kafka import KafkaProducer
from pizza import PizzaProvider

MAX_NUMBER_PIZZAS_IN_ORDER = 10
MAX_ADDITIONAL_TOPPINGS_IN_PIZZA = 5


# Creating a Faker instance and seeding to have the
# same results every time we execute the script
fake = Faker()
Faker.seed(4321)


# function produce_msgs starts producing messages with Faker
def produce_msgs(
    hostname="localhost",
    port="2902",
    topic_name="pizza-orders",
    nr_messages=-1,
    max_waiting_time_in_sec=5,
):

    producer = KafkaProducer(
        bootstrap_servers=hostname + ":" + port,
        security_protocol="PLAINTEXT",
        value_serializer=lambda v: json.dumps(v).encode("ascii"),
        key_serializer=lambda v: json.dumps(v).encode("ascii"),
    )

    if nr_messages <= 0:
        nr_messages = float("inf")
    i = 0
    fake.add_provider(PizzaProvider)
    while i < nr_messages:
        message, key = fake.produce_msg(
            fake,
            i,
            MAX_NUMBER_PIZZAS_IN_ORDER,
            MAX_ADDITIONAL_TOPPINGS_IN_PIZZA,
        )

        print("Sending: {}".format(message))
        # sending the message to Kafka
        producer.send(topic_name, key=key, value=message)
        # Sleeping time
        sleep_time = random.randint(0, int(max_waiting_time_in_sec * 10000)) / 10000
        print("Sleeping for..." + str(sleep_time) + "s")
        time.sleep(sleep_time)

        # Force flushing of all messages
        if (i % 100) == 0:
            producer.flush()
        i = i + 1
    producer.flush()


# calling the main produce_msgs function: parameters are:
#   * nr_messages: number of messages to produce
#   * max_waiting_time_in_sec: maximum waiting time in sec between messages


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        help="Kafka Host (obtained from Aiven console)",
        default="localhost",
    )
    parser.add_argument(
        "--port",
        help="Kafka Port (obtained from Aiven console)",
        default="29092",
    )
    parser.add_argument("--topic-name", help="Topic Name", default="pizza-orders")
    parser.add_argument(
        "--nr-messages",
        help="Number of messages to produce (0 for unlimited)",
        default=-1,
    )
    parser.add_argument(
        "--max-waiting-time",
        help="Max waiting time between messages (0 for none)",
        default=5,
    )

    args = parser.parse_args()
    p_hostname = args.host
    p_port = args.port
    p_topic_name = args.topic_name
    produce_msgs(
        hostname=p_hostname,
        port=p_port,
        topic_name=p_topic_name,
        nr_messages=int(args.nr_messages),
        max_waiting_time_in_sec=float(args.max_waiting_time),
    )
    print(args.nr_messages)


if __name__ == "__main__":
    main()
