import threading
import pika
import json
import uuid
import time

CLIENT_ID = str(uuid.uuid4())

EXCHANGE = "solicitudes"
RESPONSE_QUEUE = f"respuesta.{CLIENT_ID}"


def conectar_rabbit():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq", heartbeat=600)
            )
            print("Conectado a RabbitMQ")
            return connection
        except Exception as e:
            print("Esperando RabbitMQ...", e)
            time.sleep(3)


# =========================
# LISTENER
# =========================
def escuchar():
    connection = conectar_rabbit()
    channel = connection.channel()

    channel.queue_declare(
        queue=RESPONSE_QUEUE,
        exclusive=True,
        auto_delete=True
    )

    def callback(ch, method, properties, body):
        try:
            mensaje = json.loads(body)
            print("\nRESPUESTA:")
            print(json.dumps(mensaje, indent=4))
            print("> ", end="", flush=True)
        except Exception as e:
            print("Error respuesta:", e)

    channel.basic_consume(
        queue=RESPONSE_QUEUE,
        on_message_callback=callback,
        auto_ack=True
    )

    print("Escuchando respuestas...")
    channel.start_consuming()


# =========================
# SENDER
# =========================
def enviar():
    connection = conectar_rabbit()
    channel = connection.channel()

    # exchange (SIN CONFLICTOS)
    channel.exchange_declare(
        exchange=EXCHANGE,
        exchange_type="direct"
    )

    while True:
        print("\n1 Usuarios")
        print("2 Textos")
        print("3 Correos")
        print("0 Salir")

        opcion = input("> ")

        if opcion == "0":
            break

        routing = {
            "1": "usuarios",
            "2": "textos",
            "3": "correos"
        }.get(opcion)

        if not routing:
            continue

        texto = input("Mensaje: ")

        mensaje = {
            "client_id": CLIENT_ID,
            "contenido": texto
        }

        channel.basic_publish(
            exchange=EXCHANGE,
            routing_key=routing,
            body=json.dumps(mensaje)
        )

        print("Enviado ✔")


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    threading.Thread(target=escuchar, daemon=True).start()
    time.sleep(1)
    enviar()
