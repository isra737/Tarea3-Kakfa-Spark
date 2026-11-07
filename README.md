# Tarea 3 – Procesamiento de datos con Apache Spark y Kafka

Curso Big Data (202016911) – UNAD.

## Problema
Análisis de la variabilidad espacial y temporal de la temperatura del aire registrada por la red de estaciones del IDEAM (datos abiertos de Colombia, datos.gov.co), con un componente en tiempo real que simula lecturas de estaciones mediante Kafka y Spark Structured Streaming.

## Contenido
| Archivo | Descripción |
|---|---|
| `batch_analisis_spark.py` | Carga, limpieza, transformación, EDA y gráficas con PySpark (DataFrames, Spark SQL y RDD). |
| `kafka_producer.py` | Generador de lecturas simuladas que publica en el topic `sensor_data`. |
| `spark_streaming_consumer.py` | Consumidor Structured Streaming: estadísticas por ventana de 1 minuto y estación. |
| `graficar_streaming.py` | Gráfica de los resultados guardados por el consumidor. |

## Requisitos
- Máquina virtual de las tareas anteriores (Hadoop + Spark 3.5.x), Python 3.
- `pip install kafka-python pandas matplotlib`
- Kafka y ZooKeeper en ejecución y topic creado (Anexo 3):
  `/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic sensor_data`

## Ejecución en batch
```bash
# Descarga una muestra desde datos.gov.co y la procesa
spark-submit batch_analisis_spark.py --descargar --limite 500000

# O bien, con un CSV exportado manualmente del portal
spark-submit batch_analisis_spark.py --entrada temperatura.csv --salida resultados
```
Genera en `resultados/`: `resumen_batch.json`, tablas CSV y cuatro gráficas PNG.
Antes de ejecutar, verifique el identificador del recurso (`RECURSO_SOCRATA`) en el portal.

## Ejecución en tiempo real
Terminal 1 (productor):
```bash
python3 kafka_producer.py
```
Terminal 2 (consumidor):
```bash
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
```
Terminar con `Ctrl+C` y graficar:
```bash
python3 graficar_streaming.py salida_streaming fig_streaming.png
```
La interfaz web de Spark está en `http://<ip-de-la-VM>:4040`.

## Notas
Los datos del productor son simulados; no corresponden a mediciones reales.
