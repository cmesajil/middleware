# ia.py

import pika
import json
import time


def conectar_rabbit():

    while True:

        try:

            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host="rabbitmq",
                    heartbeat=600
                )
            )

            print("Conectado a RabbitMQ")

            return connection

        except Exception as e:

            print("Esperando RabbitMQ...")
            print(e)

            time.sleep(5)


connection = conectar_rabbit()

channel = connection.channel()

channel.queue_declare(
    queue="cola.ia"
)


def callback(ch, method, properties, body):

    try:

        mensaje = json.loads(body)

        print("IA:", mensaje)

        cola_cliente = f"respuesta.{mensaje['client_id']}"

        channel.queue_declare(
            queue=cola_cliente
        )

        respuesta = {
            "estado": "ok",
            "servicio": mensaje["origen"],
            "resultado": mensaje["resultado"]
        }

        channel.basic_publish(
            exchange="",
            routing_key=cola_cliente,
            body=json.dumps(respuesta)
        )

        print(f"Respuesta enviada a {cola_cliente}")

    except Exception as e:

        print("Error procesando mensaje:")
        print(e)


channel.basic_consume(
    queue="cola.ia",
    on_message_callback=callback,
    auto_ack=True
)

print("Nodo IA listo")

while True:

    try:

        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError:

        print("Conexión perdida. Reconectando...")

        connection = conectar_rabbit()
        channel = connection.channel()

        channel.queue_declare(
            queue="cola.ia"
        )

    except Exception as e:

        print("Error:")
        print(e)

        time.sleep(5)
