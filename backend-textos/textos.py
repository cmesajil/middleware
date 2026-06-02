# textos.py

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

channel.exchange_declare(
    exchange="solicitudes",
    exchange_type="direct"
)

channel.queue_declare(queue="cola.textos")

channel.queue_bind(
    exchange="solicitudes",
    queue="cola.textos",
    routing_key="textos"
)

channel.queue_declare(queue="cola.ia")


def callback(ch, method, properties, body):

    try:

        mensaje = json.loads(body)

        print("Mensaje recibido:")
        print(mensaje)

        resultado = {
            "client_id": mensaje["client_id"],
            "origen": "textos",
            "resultado": "texto procesado"
        }

        channel.basic_publish(
            exchange="",
            routing_key="cola.ia",
            body=json.dumps(resultado)
        )

        print("Enviado a cola.ia")

    except Exception as e:

        print("Error procesando mensaje:")
        print(e)


channel.basic_consume(
    queue="cola.textos",
    on_message_callback=callback,
    auto_ack=True
)

print("Backend Textos listo")

while True:

    try:

        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError:

        print("Conexión perdida. Reconectando...")

        connection = conectar_rabbit()
        channel = connection.channel()

    except Exception as e:

        print("Error:")
        print(e)

        time.sleep(5)
