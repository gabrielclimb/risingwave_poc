from faststream import FastStream
from faststream.kafka import KafkaBroker

broker = KafkaBroker("localhost:29092")


app = FastStream(broker)


@broker.publisher("out")
async def handle_msg(user: str, user_id: int) -> str:
    return f"User: {user_id} - {user} registered"
