import asyncio
from nats.aio.client import Client as NATS
from nats.aio.errors import ErrConnectionClosed, ErrTimeout


async def publish_message():
    # Подключение к серверу Nats
    nc = NATS()

    await nc.connect(servers=["nats://nats:4222"])

    # Отправка сообщения
    await nc.publish("topic", b'Your message!!!!!!!!!!!!!!!')

    # Закрытие соединения
    await nc.close()


async def subscribe_to_messages():
    async def message_handler(msg):
        print(" a message:", msg.data.decode())

    # Подключение к серверу Nats
    nc = NATS()

    await nc.connect(servers=["nats://nats:4222"])

    # Подписка на сообщения
    await nc.subscribe("topic", cb=message_handler)

    # Ожидание сообщений
    await asyncio.sleep(5)

    # Закрытие соединения
    await nc.close()


# Запуск асинхронных функций
async def main():
    await asyncio.gather(
        publish_message(),
        subscribe_to_messages()
    )

if __name__ == '__main__':
    asyncio.run(main())
