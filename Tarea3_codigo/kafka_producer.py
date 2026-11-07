#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Generador de datos para Kafka: simula lecturas de estaciones meteorológicas.
Basado en el productor del Anexo 3, con estaciones identificables, ciclo diario de
temperatura y ocasionales valores extremos para probar alertas.

Uso:
    python3 kafka_producer.py [--topic sensor_data] [--intervalo 1] [--max-mensajes 0]
"""
import argparse
import json
import math
import random
import time

from kafka import KafkaProducer

ESTACIONES = {
    1: ("Aeropuerto El Dorado", "CUNDINAMARCA", 14.0),
    2: ("Aeropuerto Olaya Herrera", "ANTIOQUIA", 24.0),
    3: ("Aeropuerto Alfonso Bonilla Aragón", "VALLE DEL CAUCA", 25.0),
    4: ("Aeropuerto Rafael Núñez", "BOLÍVAR", 29.0),
    5: ("Aeropuerto Alberto Lleras", "BOYACÁ", 16.0),
    6: ("Aeropuerto Camilo Daza", "NORTE DE SANTANDER", 28.0),
    7: ("Aeropuerto Vanguardia", "META", 27.0),
    8: ("Aeropuerto Almirante Padilla", "LA GUAJIRA", 30.0),
    9: ("Aeropuerto Gustavo Rojas Pinilla", "SAN ANDRÉS", 28.0),
    10: ("Aeropuerto Benito Salas", "HUILA", 26.0),
}  # temperatura base orientativa por estación (simulación, no son datos reales)


def generar_lectura() -> dict:
    sensor_id = random.choice(list(ESTACIONES))
    nombre, depto, base = ESTACIONES[sensor_id]
    hora = time.localtime().tm_hour + time.localtime().tm_min / 60
    ciclo = 4.0 * math.sin((hora - 9) / 24 * 2 * math.pi)  # máximo hacia las 15 h
    temp = base + ciclo + random.gauss(0, 0.8)
    if random.random() < 0.02:  # 2 % de eventos extremos
        temp += random.uniform(6, 10)
    humedad = max(20.0, min(100.0, 85 - 2.2 * (temp - base) - random.uniform(0, 15)))
    return {
        "sensor_id": sensor_id,
        "estacion": nombre,
        "departamento": depto,
        "temperature": round(temp, 2),
        "humidity": round(humedad, 2),
        "timestamp": int(time.time()),
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="sensor_data")
    ap.add_argument("--intervalo", type=float, default=1.0, help="segundos entre mensajes")
    ap.add_argument("--max-mensajes", type=int, default=0, help="0 = infinito")
    args = ap.parse_args()

    productor = KafkaProducer(
        bootstrap_servers=[args.bootstrap],
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        key_serializer=lambda k: str(k).encode("utf-8"),
        acks="all",
    )
    enviados = 0
    try:
        while args.max_mensajes == 0 or enviados < args.max_mensajes:
            lectura = generar_lectura()
            # La clave = sensor_id: todas las lecturas de una estación van a la misma partición
            productor.send(args.topic, key=lectura["sensor_id"], value=lectura)
            enviados += 1
            print(f"Enviado #{enviados}: {lectura}")
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print("\nInterrumpido por el usuario.")
    finally:
        productor.flush()
        productor.close()


if __name__ == "__main__":
    main()
