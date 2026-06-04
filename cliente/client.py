import pika
import json
import uuid
import threading
import time

CLIENT_ID = str(uuid.uuid4())

OPCIONES = {
    "1": "Solicitud de préstamo",
    "2": "Consulta de préstamo",
    "3": "Evaluación de préstamo",
    "4": "Aviso de meses no pagados",
    "5": "Aviso de término de pago",
    "6": "Refinanciamiento"
}


def conectar():
    while True:
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
        except Exception:
            time.sleep(2)


# ================= RECEIVER =================
def escuchar():
    conn = conectar()
    ch = conn.channel()

    queue = f"respuesta.{CLIENT_ID}"
    ch.queue_declare(queue=queue)

    def callback(ch, method, properties, body):
        print("\n========== RESPUESTA IA ==========")
        print(json.loads(body))
        print("==================================")

    ch.basic_consume(
        queue=queue,
        on_message_callback=callback,
        auto_ack=True
    )

    ch.start_consuming()


# ================= SENDER =================
def enviar():
    conn = conectar()
    ch = conn.channel()

    ch.queue_declare(queue="cola.main")

    while True:

        print("\nSeleccione una opción:")
        for k, v in OPCIONES.items():
            print(f"{k}. {v}")

        opcion = input("\nOpción: ").strip()

        if opcion not in OPCIONES:
            print("Opción inválida")
            continue

        mensaje = input("Mensaje: ")

        data = {
            "client_id": CLIENT_ID,
            "tipo": OPCIONES[opcion],
            "contenido": mensaje
        }

        ch.basic_publish(
            exchange="",
            routing_key="cola.main",
            body=json.dumps(data)
        )

        print("Enviado")


threading.Thread(target=escuchar, daemon=True).start()
enviar()
