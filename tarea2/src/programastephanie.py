import os
import sys
import shutil
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import funciones  # importa las funciones desde el archivo funciones.py

# crea la sesión de Spark
spark = SparkSession.builder.appName("Tarea2BigData").getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

# verifica que se hayan pasado los archivos YAML como argumentos
if len(sys.argv) < 2:
    print("por favor proporciona al menos un archivo yaml como argumento")
    sys.exit(1)

# obtiene los archivos yaml como argumentos
archivos_yamls = sys.argv[1:] 

#  crea una lista de dataframes a partir de los archivos yaml proporcionados
dataframes = []
for ruta in archivos_yamls:
    datos_yaml = funciones.leer_archivo_yml(ruta)
    df = funciones.convertir_a_dataframe(datos_yaml, spark)
    dataframes.append(df)

# une todos los dataframes
df_final = dataframes[0]
for df in dataframes[1:]:
    df_final = df_final.union(df)

# se crea la columna total_venta (cantidad * precio_unitario)
df_final = df_final.withColumn("total_venta", F.col("cantidad") * F.col("precio_unitario"))

# se calcula el total de productos vendidos
total_productos = df_final.groupBy("nombre_producto").agg(F.sum("cantidad").alias("cantidad_total"))

print("\n--- Total de productos vendidos ---")
total_productos.show()

# se calcula el total de ventas por caja
total_cajas = df_final.groupBy("numero_caja").agg(F.sum("total_venta").alias("total_vendido"))

print("\n--- Total vendido por caja ---")
total_cajas.show()

# se calcula las metricas
caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75 = funciones.calcular_metricas(df_final)
producto_mas_vendido, producto_mayor_ingreso = funciones.calcular_productos(df_final)


metricas_data = [
    ("caja_con_mas_ventas", caja_con_mas_ventas),
    ("caja_con_menos_ventas", caja_con_menos_ventas),
    ("percentil_25_por_caja", percentil_25),
    ("percentil_50_por_caja", percentil_50),
    ("percentil_75_por_caja", percentil_75),
    ("producto_mas_vendido_por_unidad", producto_mas_vendido),
    ("producto_de_mayor_ingreso", producto_mayor_ingreso)
]

df_metricas = spark.createDataFrame(metricas_data, ["Métrica", "Valor"])

print("\n--- Métricas ---")
df_metricas.show()

# elimna la carpeta si ya existe para que asi se pueda sobrescribir en cada corrida
def eliminar_carpeta_si_existe(ruta_carpeta):
    if os.path.exists(ruta_carpeta):
        shutil.rmtree(ruta_carpeta)


eliminar_carpeta_si_existe("/src/output/total_productos")
eliminar_carpeta_si_existe("/src/output/total_cajas")
eliminar_carpeta_si_existe("/src/output/metricas")

# guarda los resultados de total_productos 
total_productos.coalesce(1).write.mode("overwrite").csv("/src/output/total_productos", header=True)

# guardar los resultados de total_cajas
total_cajas.coalesce(1).write.mode("overwrite").csv("/src/output/total_cajas", header=True)

# guarda las metricas
df_metricas.coalesce(1).write.mode("overwrite").csv("/src/output/metricas", header=True)

# Finalizar la sesión de Spark
spark.stop()
