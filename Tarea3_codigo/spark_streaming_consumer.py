#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Consumidor Spark Structured Streaming para el topic de Kafka.
Calcula, por ventana de 1 minuto y por estación: número de lecturas, temperatura
promedio/mínima/máxima, humedad promedio y número de alertas (temperatura > umbral).

Ejecución (primero el productor):
    spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 \
        spark_streaming_consumer.py [--topic sensor_data] [--umbral 35]
"""
import argparse
import time

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, IntegerType, LongType, StringType,
                               StructField, StructType)

ESQUEMA = StructType([
    StructField("sensor_id", IntegerType()),
    StructField("estacion", StringType()),
    StructField("departamento", StringType()),
    StructField("temperature", DoubleType()),
    StructField("humidity", DoubleType()),
    StructField("timestamp", LongType()),  # segundos desde epoch
])


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--topic", default="sensor_data")
    ap.add_argument("--umbral", type=float, default=35.0, help="°C para contar alertas")
    ap.add_argument("--salida", default="salida_streaming")
    args = ap.parse_args()

    spark = SparkSession.builder.appName("SensorDataAnalysis").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", "4")

    # 1) Lectura del topic
    crudo = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2) Decodificación del JSON; el epoch en segundos se convierte a timestamp
    lecturas = (
        crudo.select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("d"))
        .select("d.*")
        .withColumn("evento_ts", F.col("timestamp").cast("timestamp"))
        .dropna(subset=["sensor_id", "temperature", "evento_ts"])
    )

    # 3) Agregación por ventana de 1 minuto y estación, con marca de agua de 2 minutos
    estadisticas = (
        lecturas.withWatermark("evento_ts", "2 minutes")
        .groupBy(F.window("evento_ts", "1 minute"), "sensor_id", "estacion")
        .agg(
            F.count("*").alias("lecturas"),
            F.round(F.avg("temperature"), 2).alias("temp_prom"),
            F.round(F.min("temperature"), 2).alias("temp_min"),
            F.round(F.max("temperature"), 2).alias("temp_max"),
            F.round(F.avg("humidity"), 2).alias("hum_prom"),
            F.sum(F.when(F.col("temperature") > args.umbral, 1).otherwise(0)).alias("alertas"),
        )
        .select(
            F.col("window.start").alias("ventana_inicio"),
            F.col("window.end").alias("ventana_fin"),
            "sensor_id", "estacion", "lecturas", "temp_prom", "temp_min",
            "temp_max", "hum_prom", "alertas",
        )
    )

    # 4) Cada micro-lote: se muestra en consola y se guarda en CSV para graficar
    def procesar_lote(lote, id_lote):
        if lote.rdd.isEmpty():
            return
        print(f"\n===== Micro-lote {id_lote} =====")
        lote.orderBy("ventana_inicio", "sensor_id").show(20, truncate=False)
        (lote.withColumn("id_lote", F.lit(id_lote))
             .write.mode("append").option("header", True).csv(args.salida))

    consulta = (
        estadisticas.writeStream.outputMode("update")
        .foreachBatch(procesar_lote)
        .option("checkpointLocation", f"/tmp/chk_sensor_{int(time.time())}")
        .trigger(processingTime="10 seconds")
        .start()
    )
    consulta.awaitTermination()


if __name__ == "__main__":
    main()
