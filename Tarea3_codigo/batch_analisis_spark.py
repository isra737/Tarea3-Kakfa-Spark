#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tarea 3 - Big Data (UNAD 202016911)
Procesamiento en batch con Apache Spark (PySpark)

Problema: variabilidad espacial y temporal de la temperatura del aire registrada
por la red de estaciones del IDEAM (datos abiertos, datos.gov.co).

Uso:
    # Opción A: descargar una muestra directamente desde la fuente original (Socrata)
    spark-submit batch_analisis_spark.py --descargar --limite 500000

    # Opción B: usar un CSV ya descargado desde datos.gov.co (Exportar > CSV)
    spark-submit batch_analisis_spark.py --entrada /ruta/temperatura.csv

Salidas (carpeta --salida, por defecto ./resultados):
    resumen_batch.json, tablas CSV y gráficas PNG.
"""
import argparse
import json
import os
import time
import unicodedata
import urllib.parse
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# ----------------------------------------------------------------------------
# Configuración
# ----------------------------------------------------------------------------
# Identificador del recurso en datos.gov.co. VERIFICAR en el portal antes de ejecutar
# (buscar: "Datos Hidrometeorológicos Crudos - Red de Estaciones IDEAM : Temperatura").
RECURSO_SOCRATA = "sbwg-7ju4"
URL_BASE = "https://www.datos.gov.co/resource/{}.csv"

# Nombres lógicos -> posibles nombres de columna en el archivo (ya normalizados:
# minúsculas, sin tildes, sin espacios ni guiones). Evita fallos por diferencias de formato.
ALIAS_COLUMNAS = {
    "estacion_cod": ["codigoestacion", "codigo_estacion", "codestacion"],
    "estacion": ["nombreestacion", "nombre_estacion", "estacion"],
    "sensor": ["descripcionsensor", "descripcion_sensor", "sensor"],
    "fecha": ["fechaobservacion", "fecha_observacion", "fecha"],
    "valor": ["valorobservado", "valor_observado", "valor"],
    "unidad": ["unidadmedida", "unidad_medida", "unidad"],
    "departamento": ["departamento"],
    "municipio": ["municipio"],
    "latitud": ["latitud", "lat"],
    "longitud": ["longitud", "lon", "lng"],
}
OBLIGATORIAS = ["fecha", "valor", "departamento", "municipio", "estacion"]

# Rango físicamente plausible para temperatura del aire en Colombia (°C)
T_MIN, T_MAX = -10.0, 50.0


# ----------------------------------------------------------------------------
# Utilidades
# ----------------------------------------------------------------------------
def normalizar(nombre: str) -> str:
    """minúsculas, sin tildes, sin espacios/guiones/puntos."""
    s = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return "".join(ch for ch in s.lower() if ch.isalnum() or ch == "_")


def descargar_csv(destino: str, limite: int, bloque: int = 50000) -> str:
    """Descarga `limite` filas desde la API SODA de datos.gov.co, paginando."""
    url_base = URL_BASE.format(RECURSO_SOCRATA)
    os.makedirs(os.path.dirname(os.path.abspath(destino)), exist_ok=True)
    descargadas = 0
    with open(destino, "wb") as out:
        primera = True
        while descargadas < limite:
            n = min(bloque, limite - descargadas)
            params = urllib.parse.urlencode(
                {"$limit": n, "$offset": descargadas, "$order": ":id"}, safe="$:"
            )
            with urllib.request.urlopen(f"{url_base}?{params}", timeout=120) as r:
                datos = r.read()
            lineas = datos.split(b"\n", 1)
            if len(lineas) < 2 or not lineas[1].strip():
                break  # no hay más filas
            out.write(datos if primera else lineas[1])
            primera = False
            descargadas += n
            print(f"  descargadas ~{descargadas} filas")
            time.sleep(0.5)  # cortesía con el servicio
    return destino


def mapear_columnas(df):
    """Renombra columnas a nombres lógicos usando ALIAS_COLUMNAS."""
    normales = {normalizar(c): c for c in df.columns}
    for logico, candidatos in ALIAS_COLUMNAS.items():
        for cand in candidatos:
            if cand in normales:
                df = df.withColumnRenamed(normales[cand], logico)
                break
    faltan = [c for c in OBLIGATORIAS if c not in df.columns]
    if faltan:
        raise SystemExit(
            f"Columnas obligatorias no encontradas: {faltan}\n"
            f"Columnas del archivo: {df.columns}\n"
            "Ajuste ALIAS_COLUMNAS en el script."
        )
    return df


def guardar_csv(df, ruta):
    """Guarda un DataFrame pequeño (agregado) como un único CSV legible."""
    df.coalesce(1).write.mode("overwrite").option("header", True).csv(ruta)


# ----------------------------------------------------------------------------
# Programa principal
# ----------------------------------------------------------------------------
def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--entrada", help="CSV local con los datos")
    ap.add_argument("--descargar", action="store_true", help="descargar desde datos.gov.co")
    ap.add_argument("--limite", type=int, default=500000, help="filas a descargar")
    ap.add_argument("--salida", default="resultados")
    args = ap.parse_args()

    os.makedirs(args.salida, exist_ok=True)
    entrada = args.entrada
    if args.descargar:
        entrada = os.path.join(args.salida, "temperatura_ideam.csv")
        print("Descargando datos desde la fuente original...")
        descargar_csv(entrada, args.limite)
    if not entrada:
        raise SystemExit("Indique --entrada o --descargar")

    spark = (
        SparkSession.builder.appName("Tarea3_Batch_Temperatura_IDEAM")
        .config("spark.sql.shuffle.partitions", "8")  # VM pequeña: menos particiones
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    resumen = {}

    # ------------------------------------------------------------------ 1. Carga
    crudo = spark.read.option("header", True).option("inferSchema", False).csv(entrada)
    crudo = mapear_columnas(crudo)
    resumen["registros_crudos"] = crudo.count()
    print("Esquema original:")
    crudo.printSchema()

    # -------------------------------------------------------------- 2. Limpieza
    df = (
        crudo.withColumn("valor", F.col("valor").cast("double"))
        .withColumn("fecha", F.col("fecha").cast("timestamp"))
        .withColumn("departamento", F.upper(F.trim(F.col("departamento"))))
        .withColumn("municipio", F.upper(F.trim(F.col("municipio"))))
    )
    nulos = df.select(
        [F.sum(F.col(c).isNull().cast("int")).alias(c) for c in ("fecha", "valor", "departamento", "municipio")]
    ).first().asDict()
    resumen["nulos_por_columna"] = nulos

    # Si el recurso mezcla sensores, conservar solo los de temperatura del aire
    if "sensor" in df.columns:
        df = df.filter(F.upper(F.col("sensor")).contains("TEMP"))

    antes = df.count()
    df = df.dropna(subset=["fecha", "valor", "departamento", "municipio"])
    resumen["tras_eliminar_nulos"] = df.count()

    df = df.dropDuplicates(["estacion", "fecha", "valor"])
    resumen["tras_eliminar_duplicados"] = df.count()

    df = df.filter((F.col("valor") >= T_MIN) & (F.col("valor") <= T_MAX))
    resumen["tras_filtro_rango"] = df.count()
    resumen["descartados_total"] = resumen["registros_crudos"] - resumen["tras_filtro_rango"]

    # ---------------------------------------------------------- 3. Transformación
    df = (
        df.withColumn("anio", F.year("fecha"))
        .withColumn("mes", F.month("fecha"))
        .withColumn("hora", F.hour("fecha"))
    ).cache()  # se reutiliza en varias agregaciones
    resumen["registros_limpios"] = df.count()
    df.createOrReplaceTempView("temperatura")

    # ------------------------------------------------------------------- 4. EDA
    est = df.select(
        F.count("*").alias("n"),
        F.round(F.mean("valor"), 2).alias("media"),
        F.round(F.stddev("valor"), 2).alias("desv"),
        F.min("valor").alias("minimo"),
        F.expr("percentile_approx(valor, 0.5)").alias("mediana"),
        F.max("valor").alias("maximo"),
        F.countDistinct("estacion").alias("estaciones"),
        F.countDistinct("departamento").alias("departamentos"),
        F.min("fecha").alias("fecha_ini"),
        F.max("fecha").alias("fecha_fin"),
    ).first().asDict()
    est["fecha_ini"], est["fecha_fin"] = str(est["fecha_ini"]), str(est["fecha_fin"])
    resumen["estadisticas_globales"] = est

    por_depto = (
        df.groupBy("departamento")
        .agg(F.round(F.avg("valor"), 2).alias("temp_media"),
             F.round(F.max("valor"), 2).alias("temp_max"),
             F.count("*").alias("n"))
        .orderBy(F.desc("temp_media"))
    )
    por_mes = (
        df.groupBy("anio", "mes").agg(F.round(F.avg("valor"), 2).alias("temp_media"))
        .orderBy("anio", "mes")
    )
    por_hora = (
        df.groupBy("hora").agg(F.round(F.avg("valor"), 2).alias("temp_media")).orderBy("hora")
    )
    top_muni = (
        spark.sql(
            "SELECT municipio, departamento, ROUND(AVG(valor),2) AS temp_media, "
            "COUNT(*) AS n FROM temperatura GROUP BY municipio, departamento "
            "HAVING COUNT(*) >= 100 ORDER BY temp_media DESC LIMIT 10"
        )
    )

    # Histograma por intervalos de 2 °C
    hist = (
        df.withColumn("intervalo", (F.floor(F.col("valor") / 2) * 2).cast("int"))
        .groupBy("intervalo").count().orderBy("intervalo")
    )

    # Versión con RDD (map + reduceByKey): promedio por departamento
    rdd_prom = (
        df.select("departamento", "valor").rdd
        .map(lambda r: (r["departamento"], (r["valor"], 1)))
        .reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))
        .mapValues(lambda s: round(s[0] / s[1], 2))
    )
    resumen["rdd_primeros_5_departamentos"] = rdd_prom.take(5)

    print("\n=== Resumen estadístico global ===")
    for k, v in est.items():
        print(f"{k:>15}: {v}")
    print("\n=== Temperatura media por departamento (top 10) ===")
    por_depto.show(10, truncate=False)
    print("=== Municipios con mayor temperatura media (top 10) ===")
    top_muni.show(truncate=False)

    # --------------------------------------------------------- 5. Almacenamiento
    for nombre, tabla in [("por_departamento", por_depto), ("por_mes", por_mes),
                          ("por_hora", por_hora), ("top_municipios", top_muni),
                          ("histograma", hist)]:
        guardar_csv(tabla, os.path.join(args.salida, f"tabla_{nombre}"))

    with open(os.path.join(args.salida, "resumen_batch.json"), "w", encoding="utf-8") as f:
        json.dump(resumen, f, ensure_ascii=False, indent=2, default=str)

    # ------------------------------------------------------------ 6. Visualización
    # Los agregados son pequeños: se llevan al driver para graficar con matplotlib.
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    pd_depto = por_depto.limit(15).toPandas()
    pd_mes = por_mes.toPandas()
    pd_hora = por_hora.toPandas()
    pd_hist = hist.toPandas()

    fig, ax = plt.subplots(figsize=(9, 5))
    ax.barh(pd_depto["departamento"][::-1], pd_depto["temp_media"][::-1], color="#1F6F8B")
    ax.set_xlabel("Temperatura media (°C)")
    ax.set_title("Quince departamentos con mayor temperatura media")
    fig.tight_layout(); fig.savefig(os.path.join(args.salida, "fig_departamentos.png"), dpi=150)

    pd_mes["periodo"] = pd_mes["anio"].astype(str) + "-" + pd_mes["mes"].astype(str).str.zfill(2)
    fig, ax = plt.subplots(figsize=(9, 4))
    ax.plot(pd_mes["periodo"], pd_mes["temp_media"], marker="o", color="#C0392B")
    ax.set_ylabel("Temperatura media (°C)"); ax.set_title("Evolución mensual")
    ax.tick_params(axis="x", rotation=60)
    fig.tight_layout(); fig.savefig(os.path.join(args.salida, "fig_serie_mensual.png"), dpi=150)

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.plot(pd_hora["hora"], pd_hora["temp_media"], marker="o", color="#2E7D32")
    ax.set_xlabel("Hora del día"); ax.set_ylabel("Temperatura media (°C)")
    ax.set_title("Perfil horario de la temperatura")
    fig.tight_layout(); fig.savefig(os.path.join(args.salida, "fig_perfil_horario.png"), dpi=150)

    fig, ax = plt.subplots(figsize=(8, 4))
    ax.bar(pd_hist["intervalo"], pd_hist["count"], width=1.8, color="#6A4C93")
    ax.set_xlabel("Temperatura (°C, intervalos de 2)"); ax.set_ylabel("Registros")
    ax.set_title("Distribución de las observaciones")
    fig.tight_layout(); fig.savefig(os.path.join(args.salida, "fig_histograma.png"), dpi=150)

    print(f"\nListo. Resultados en: {os.path.abspath(args.salida)}")
    df.unpersist()
    spark.stop()


if __name__ == "__main__":
    main()
