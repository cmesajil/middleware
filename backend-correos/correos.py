import pika
import json
import time
import psycopg2
import re
from datetime import datetime
from datetime import timedelta

# ================= REGEX =================

REGEX_CORREO = r'[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}'
REGEX_DNI = r'\b\d{8}\b'
REGEX_MONTO = r'(?:prestamo|préstamo|credito|crédito|monto|s\/\.|\$)\s*(?:de\s*)?(\d+(?:\.\d{1,2})?)'

# ================= DB =================

def obtener_prestamo(cursor, id_usuario):

    cursor.execute(
        """
        SELECT
            id_prestamo,
            monto,
            tasa_interes,
            plazo,
            estado
        FROM prestamos
        WHERE id_usuario=%s
        ORDER BY id_prestamo DESC
        LIMIT 1
        """,
        (id_usuario,)
    )

    return cursor.fetchone()

def crear_prestamo(cursor, id_usuario, monto):

    fecha_inicio = datetime.now()
    fecha_fin = fecha_inicio + timedelta(days=365)

    cursor.execute(
        """
        INSERT INTO prestamos
        (
            id_usuario,
            monto,
            tasa_interes,
            plazo,
            estado,
            fecha_inicio,
            fecha_fin,
            tipo_prestamo
        )
        VALUES
        (
            %s,%s,%s,%s,%s,%s,%s,%s
        )
        RETURNING id_prestamo
        """,
        (
            id_usuario,
            monto,
            12.5,
            12,
            "PENDIENTE",
            fecha_inicio,
            fecha_fin,
            "PERSONAL"
        )
    )

    return cursor.fetchone()[0]

def get_conn():
    return psycopg2.connect(
        host="postgres",
        database="postgres",
        user="postgres",
        password="root",
        port=5432
    )


def buscar_usuario(cursor, dni=None, correo=None):

    if correo and dni:
        cursor.execute(
            """
            SELECT id_usuario, dni, correo
            FROM usuarios
            WHERE correo = %s
               OR dni = %s
            LIMIT 1
            """,
            (correo, dni)
        )

    elif correo:
        cursor.execute(
            """
            SELECT id_usuario, dni, correo
            FROM usuarios
            WHERE correo = %s
            LIMIT 1
            """,
            (correo,)
        )

    elif dni:
        cursor.execute(
            """
            SELECT id_usuario, dni, correo
            FROM usuarios
            WHERE dni = %s
            LIMIT 1
            """,
            (dni,)
        )

    else:
        return None

    return cursor.fetchone()


def actualizar_datos_faltantes(cursor, id_usuario, dni, correo):

    if dni:
        cursor.execute(
            """
            UPDATE usuarios
            SET dni = COALESCE(dni, %s)
            WHERE id_usuario = %s
            """,
            (dni, id_usuario)
        )

    if correo:
        cursor.execute(
            """
            UPDATE usuarios
            SET correo = COALESCE(correo, %s)
            WHERE id_usuario = %s
            """,
            (correo, id_usuario)
        )


def crear_usuario(cursor, dni, correo):

    cursor.execute(
        """
        INSERT INTO usuarios
        (
            nombre,
            dni,
            correo,
            fecha_registro
        )
        VALUES
        (
            %s,
            %s,
            %s,
            %s
        )
        RETURNING id_usuario
        """,
        (
            "Cliente Nuevo",
            dni,
            correo,
            datetime.now()
        )
    )

    return cursor.fetchone()[0]


def crear_cuenta(cursor, id_usuario):

    cursor.execute(
        """
        INSERT INTO cuentas
        (
            id_usuario,
            saldo,
            estado,
            fecha_apertura,
            tipo_cuenta
        )
        VALUES
        (
            %s,
            %s,
            %s,
            NOW(),
            %s
        )
        RETURNING id_cuenta
        """,
        (
            id_usuario,
            0,
            "ACTIVA",
            "AHORRO"
        )
    )

    return cursor.fetchone()[0]


def obtener_cuenta(cursor, id_usuario):

    cursor.execute(
        """
        SELECT
            id_cuenta,
            saldo,
            estado,
            tipo_cuenta
        FROM cuentas
        WHERE id_usuario = %s
        LIMIT 1
        """,
        (id_usuario,)
    )

    return cursor.fetchone()


def guardar_mensaje(cursor, id_usuario, contenido, tipo):

    cursor.execute(
        """
        INSERT INTO mensajes_texto
        (
            id_usuario,
            contenido,
            fecha_envio,
            tipo_mensaje,
            canal
        )
        VALUES
        (
            %s,
            %s,
            NOW(),
            %s,
            %s
        )
        """,
        (
            id_usuario,
            contenido,
            tipo,
            "RABBITMQ"
        )
    )


# ================= RABBIT =================

def conectar_rabbit():

    while True:
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
        except Exception as e:
            print("Esperando RabbitMQ...", e)
            time.sleep(2)


connection = conectar_rabbit()
channel = connection.channel()

channel.queue_declare(queue="cola.main")
channel.queue_declare(queue="cola.ia")

print("Backend conectado a RabbitMQ")


# ================= CALLBACK =================

def callback(ch, method, properties, body):
    msg = json.loads(body)

    print("\n================================")
    print(msg)
    print("================================")

    client_id = msg["client_id"]
    contenido = msg["contenido"]
    tipo = msg.get("tipo", "GENERAL")
    queue_respuesta = f"respuesta.{client_id}"

    # Asegurar que la cola de respuesta exista antes de cualquier excepción
    channel.queue_declare(queue=queue_respuesta)

    correo_match = re.search(REGEX_CORREO, contenido)
    dni_match = re.search(REGEX_DNI, contenido)
    monto_match = re.search(REGEX_MONTO, contenido, re.IGNORECASE)

    correo = correo_match.group(0) if correo_match else None
    dni = dni_match.group(0) if dni_match else None
    monto = float(monto_match.group(1)) if monto_match else None

    try:
        conn = get_conn()
        cursor = conn.cursor()

        # ====================================
        # VALIDACIÓN 1: VALIDA IDENTIDAD
        # ====================================
        if not correo and not dni:
            respuesta = {
                "estado": "ERROR",
                "mensaje": "No pudimos identificar quién eres. Por favor, vuelve a enviar tu mensaje incluyendo tu DNI o Correo."
            }
            channel.basic_publish(exchange="", routing_key=queue_respuesta, body=json.dumps(respuesta))
            print("[ALERTA] Falta DNI o Correo. Respuesta enviada al cliente.")
            cursor.close()
            conn.close()
            return # <--- CORRECCIÓN: Detiene el flujo aquí.

        # ====================================
        # VALIDACIÓN 2: VALIDA MONTO
        # ====================================
        elif monto is None:
            respuesta = {
                "estado": "PENDIENTE",
                "mensaje": "Hemos validado tus datos de perfil, pero requerimos que indiques el monto solicitado. Ejemplo: préstamo de 5000"
            }
            channel.basic_publish(exchange="", routing_key=queue_respuesta, body=json.dumps(respuesta))
            print("[ALERTA] Falta Monto. Respuesta enviada al cliente.")
            cursor.close()
            conn.close()
            return # <--- CORRECCIÓN: Detiene el flujo aquí.

        # ====================================
        # PROCESAMIENTO DE SOLICITUD
        # ====================================
        usuario = buscar_usuario(cursor, dni=dni, correo=correo)

        # ====================================
        # CASE: USUARIO EXISTENTE
        # ====================================
        if usuario:
            id_usuario = usuario[0]

            actualizar_datos_faltantes(cursor, id_usuario, dni, correo)
            guardar_mensaje(cursor, id_usuario, contenido, tipo)

            cuenta = obtener_cuenta(cursor, id_usuario)
            prestamo = obtener_prestamo(cursor, id_usuario)

            if prestamo:
                datos_prestamo = {
                    "id_prestamo": prestamo[0],
                    "monto": float(prestamo[1]),
                    "tasa": float(prestamo[2]),
                    "plazo": prestamo[3],
                    "estado": prestamo[4]
                }
            else:
                id_prestamo = crear_prestamo(cursor, id_usuario, monto)
                datos_prestamo = {
                    "id_prestamo": id_prestamo,
                    "monto": monto,
                    "estado": "PENDIENTE"
                }

            conn.commit() # Commit unificado al final del bloque exitoso

            respuesta = {
                "estado": "OK",
                "mensaje": "Usuario encontrado.",
                "id_usuario": id_usuario,
                "cuenta": {
                    "id_cuenta": cuenta[0] if cuenta else None,
                    "saldo": float(cuenta[1]) if cuenta else 0.0,
                    "estado": cuenta[2] if cuenta else "INEXISTENTE",
                    "tipo": cuenta[3] if cuenta else "NINGUNO"
                },
                "prestamo": datos_prestamo
            }

        # ====================================
        # CASE: USUARIO NUEVO
        # ====================================
        else:
            id_usuario = crear_usuario(cursor, dni, correo)
            id_cuenta = crear_cuenta(cursor, id_usuario)
            id_prestamo = crear_prestamo(cursor, id_usuario, monto)
            guardar_mensaje(cursor, id_usuario, contenido, tipo)

            cuenta = obtener_cuenta(cursor, id_usuario)

            conn.commit() # Commit una vez que todo el set de inserciones terminó sin errores

            respuesta = {
                "estado": "OK",
                "mensaje": "Cuenta de usuario y solicitud de préstamo creados correctamente.",
                "id_usuario": id_usuario,
                "cuenta": {
                    "id_cuenta": cuenta[0],
                    "saldo": float(cuenta[1]),
                    "estado": cuenta[2],
                    "tipo": cuenta[3]
                },
                "prestamo": {
                    "id_prestamo": id_prestamo,
                    "monto": monto,
                    "estado": "PENDIENTE"
                }
            }

        # ====================================
        # PROCESAMIENTO EXITOSO -> ENVÍO A IA Y CLIENTE
        # ====================================
        mensaje_ia = {
            "client_id": client_id,
            "id_usuario": respuesta.get("id_usuario"),
            "tipo": tipo,
            "contenido": contenido,
            "cuenta": respuesta.get("cuenta"),
            "prestamo": respuesta.get("prestamo")
        }

        # Notificar al nodo de IA
        channel.basic_publish(
            exchange="",
            routing_key="cola.ia",
            body=json.dumps(mensaje_ia)
        )

        # Notificar la respuesta exitosa al cliente
        channel.basic_publish(
            exchange="",
            routing_key=queue_respuesta,
            body=json.dumps(respuesta)
        )
        print("Procesamiento exitoso. Respuesta enviada al Cliente y a cola.ia")

        cursor.close()
        conn.close()

        # ANTES DE SALIR DEL TRY: Envías la confirmación manual a RabbitMQ
        ch.basic_ack(delivery_tag=method.delivery_tag)
        print("Procesamiento exitoso. Mensaje confirmado en RabbitMQ.")

    except Exception as e:
        print("ERROR:", e)
        try:
            conn.rollback()
        except:
            pass

        respuesta = {
            "estado": "ERROR",
            "mensaje": f"Ocurrió un error interno en el servidor: {str(e)}"
        }

        channel.basic_publish(
            exchange="",
            routing_key=queue_respuesta,
            body=json.dumps(respuesta)
        )

        # EN CASO DE ERROR: También confirmamos (o rechazamos con basic_nack)
        # para que la cola no se quede trabada con un mensaje corrupto
        ch.basic_ack(delivery_tag=method.delivery_tag)






# ================= START =================

channel.basic_consume(
    queue="cola.main",
    on_message_callback=callback,
    auto_ack=False # <--- CAMBIADO A FALSE (Ahora confirmas manualmente arriba)
)

print("Backend listo y escuchando cola.main")

channel.start_consuming()
