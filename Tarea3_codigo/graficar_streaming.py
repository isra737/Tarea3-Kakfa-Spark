#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Genera gráficas a partir de los CSV que escribe spark_streaming_consumer.py.
Uso: python3 graficar_streaming.py [carpeta_csv] [archivo_png]"""
import glob
import sys

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import pandas as pd

carpeta = sys.argv[1] if len(sys.argv) > 1 else "salida_streaming"
destino = sys.argv[2] if len(sys.argv) > 2 else "fig_streaming.png"

archivos = glob.glob(f"{carpeta}/*.csv")
if not archivos:
    sys.exit(f"No hay CSV en {carpeta}. Ejecute primero el consumidor.")
df = pd.concat((pd.read_csv(a) for a in archivos), ignore_index=True)
df["ventana_inicio"] = pd.to_datetime(df["ventana_inicio"])

# En modo 'update' una ventana puede aparecer varias veces: se conserva la última versión
df = (df.sort_values("id_lote")
        .drop_duplicates(["ventana_inicio", "sensor_id"], keep="last")
        .sort_values("ventana_inicio"))

fig, (a1, a2) = plt.subplots(2, 1, figsize=(10, 7), sharex=True)
for sid, g in df.groupby("sensor_id"):
    a1.plot(g["ventana_inicio"], g["temp_prom"], marker=".", label=f"Sensor {sid}")
a1.set_ylabel("Temperatura promedio (°C)")
a1.set_title("Temperatura promedio por ventana de 1 minuto")
a1.legend(ncol=5, fontsize=8)

tot = df.groupby("ventana_inicio")["lecturas"].sum()
a2.bar(tot.index, tot.values, width=0.0006, color="#1F6F8B")
a2.set_ylabel("Lecturas por ventana")
a2.set_xlabel("Inicio de la ventana")
fig.autofmt_xdate()
fig.tight_layout()
fig.savefig(destino, dpi=150)
print(f"Gráfica guardada en {destino}")
