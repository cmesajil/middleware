import pika
import json
import time

# ==========================
# CONEXIÓN RABBITMQ
# ==========================
def conectar():
    while True:
        try:
            return pika.BlockingConnection(
                pika.ConnectionParameters(host="rabbitmq")
            )
        except Exception as e:
            print("Esperando RabbitMQ...", e)
            time.sleep(2)

connection = conectar()
channel = connection.channel()
channel.queue_declare(queue="cola.ia")

print("IA conectada a RabbitMQ")
print("Escuchando cola.ia...")

# ==========================
# CALLBACK
# ==========================
def callback(ch, method, properties, body):
    print("\n========================")
    print("MENSAJE RECIBIDO EN IA")
    print("========================")

    try:
        msg = json.loads(body)
        print(json.dumps(msg, indent=4, ensure_ascii=False))

        client_id = msg["client_id"]
        tipo_solicitud = msg.get("tipo", "GENERAL")
        prestamo = msg.get("prestamo")
        cuenta = msg.get("cuenta")

        # Variables de respaldo por si no vienen datos en el JSON
        id_prestamo = prestamo.get('id_prestamo') if prestamo else "N/A"
        monto_prestamo = prestamo.get('monto') if prestamo else 0.0
        estado_prestamo = prestamo.get('estado') if prestamo else "N/A"

        saldo_cuenta = cuenta.get('saldo') if cuenta else 0.0
        id_cuenta = cuenta.get('id_cuenta') if cuenta else "N/A"

        # ========================================================
        # LÓGICA DE RESPUESTAS SEGÚN EL TIPO SELECCIONADO
        # ========================================================

        if tipo_solicitud == "Solicitud de préstamo":
            respuesta = f"Solicitud procesada. Su préstamo #{id_prestamo} por S/.{monto_prestamo} ha sido registrado correctamente y se encuentra en estado {estado_prestamo}."

        elif tipo_solicitud == "Consulta de préstamo":
            if prestamo:
                respuesta = f"Estado de Cuenta: Actualmente mantiene el préstamo #{id_prestamo} por un monto de S/.{monto_prestamo}. Estado actual: {estado_prestamo}."
            else:
                respuesta = "Consulta de préstamo: No registra ninguna solicitud de préstamo activa asociada a sus datos."

        elif tipo_solicitud == "Evaluación de préstamo":
            if monto_prestamo > 50000:
                respuesta = f"Evaluación de préstamo #{id_prestamo}: El monto solicitado (S/.{monto_prestamo}) excede el límite de aprobación inmediata. Su caso pasará a junta de créditos de riesgo."
            else:
                respuesta = f"Evaluación de préstamo #{id_prestamo}: Basado en su estado de cuenta activo con saldo S/.{saldo_cuenta}, su solicitud por S/.{monto_prestamo} califica para una aprobación preliminar."

        elif tipo_solicitud == "Aviso de meses no pagados":
            # Aquí podrías calcular cuotas basadas en tu lógica de negocio
            respuesta = f"Reporte de Morosidad: Alerta. Detectamos cuotas vencidas en su préstamo #{id_prestamo}. Evite penalizaciones e intereses moratorios realizando el pago a su cuenta #{id_cuenta}."

        elif tipo_solicitud == "Aviso de término de pago":
            respuesta = f"Liquidación Exitosa: Se confirma la liquidación completa del préstamo #{id_prestamo}. Su registro se encuentra en estado 'FINALIZADO'. ¡Gracias por su puntualidad!"

        elif tipo_solicitud == "Refinanciamiento":
            monto_refinanciado = monto_prestamo * 1.10 # Ejemplo de cálculo lógico (10% de interés de reestructuración)
            respuesta = f"Propuesta de Refinanciamiento: Evaluando su préstamo #{id_prestamo}, ofrecemos una reestructuración de su deuda por un monto total estimado de S/.{monto_refinanciado:.2f} en un nuevo plazo extendido."

        else:
            respuesta = f"Hola. Hemos recibido tu mensaje bajo la categoría '{tipo_solicitud}'. Un asesor se comunicará contigo pronto."

        # ========================================================
        # ENVÍO DE RESPUESTA AL CLIENTE
        # ========================================================
        response = {
            "client_id": client_id,
            "respuesta": respuesta
        }

        print("\nRespuesta enviada:")
        print(json.dumps(response, indent=4, ensure_ascii=False))

        channel.basic_publish(
            exchange="",
            routing_key=f"respuesta.{client_id}",
            body=json.dumps(response)
        )

    except Exception as e:
        print("ERROR IA:", e)

# ==========================
# CONSUMIDOR
# ==========================
channel.basic_consume(
    queue="cola.ia",
    on_message_callback=callback,
    auto_ack=True
)

print("IA lista y procesando mensajes dinámicos...")
channel.start_consuming()
