import pika
import json
import time
import os
import psycopg2

# =========================
# CONFIG POSTGRES
# =========================

DB_CONFIG = {
    "host": os.getenv("POSTGRES_HOST", "postgres"),
    "database": os.getenv("POSTGRES_DB", "postgres"),
    "user": os.getenv("POSTGRES_USER", "postgres"),
    "password": os.getenv("POSTGRES_PASSWORD", "root"),
    "port": int(os.getenv("POSTGRES_PORT", 5432)),
}


def conectar_db():
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            print("Conectado a PostgreSQL")
            return conn
        except Exception as e:
            print("Esperando PostgreSQL...")
            print(e)
            time.sleep(5)


db = conectar_db()
cursor = db.cursor()

# crear tabla si no existe
cursor.execute("""
CREATE TABLE IF NOT EXISTS correos (
    id SERIAL PRIMARY KEY,
    client_id TEXT,
    mensaje JSONB,
    estado TEXT DEFAULT 'procesado',
    created_at TIMESTAMP DEFAULT NOW()
)
""")
db.commit()


# =========================
# CONFIG RABBITMQ
# =========================

def conectar_rabbit():
    while True:
        try:
            connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=os.getenv("RABBIT_HOST", "rabbitmq"),
                    heartbeat=600
                )
            )
            print("Conectado a RabbitMQ")
            return connection
        except Exception as e:
            print("Esperando RabbitMQ...")
            print(e)
            time.sleep(5)


def obtener_channel(connection):
    channel = connection.channel()

    channel.exchange_declare(
        exchange="solicitudes",
        exchange_type="direct"
    )

    channel.queue_declare(queue="cola.correos")

    channel.queue_bind(
        exchange="solicitudes",
        queue="cola.correos",
        routing_key="correos"
    )

    channel.queue_declare(queue="cola.ia")

    return channel


connection = conectar_rabbit()
channel = obtener_channel(connection)


# =========================
# CALLBACK
# =========================

def callback(ch, method, properties, body):
    global db, cursor

    try:
        mensaje = json.loads(body)
        print("Mensaje recibido:", mensaje)

        # =========================
        # GUARDAR EN POSTGRES
        # =========================
        cursor.execute(
            "INSERT INTO correos (client_id, mensaje) VALUES (%s, %s)",
            (mensaje["client_id"], json.dumps(mensaje))
        )
        db.commit()

        # =========================
        # RESPUESTA A IA
        # =========================
        resultado = {
            "client_id": mensaje["client_id"],
            "origen": "correos",
            "resultado": "correo procesado"
        }

        ch.basic_publish(
            exchange="",
            routing_key="cola.ia",
            body=json.dumps(resultado)
        )

        print("Enviado a cola.ia")

    except Exception as e:
        print("Error procesando mensaje:", e)


channel.basic_consume(
    queue="cola.correos",
    on_message_callback=callback,
    auto_ack=True
)

print("Backend Correos listo")

# =========================
# LOOP ROBUSTO
# =========================

while True:
    try:
        channel.start_consuming()

    except pika.exceptions.AMQPConnectionError:
        print("Conexión Rabbit perdida. Reconectando...")
        connection = conectar_rabbit()
        channel = obtener_channel(connection)

    except Exception as e:
        print("Error general:", e)
        time.sleep(5)
