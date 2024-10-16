import sys
import os
import shutil
import glob
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import funciones  # Importar las funciones desde el archivo funciones.py

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Tarea2BigData").getOrCreate()

# Deshabilitar los logs innecesarios de Spark
spark.sparkContext.setLogLevel("ERROR")

# Expandir el patrón 'caja*.yaml' automáticamente
archivos_yamls = glob.glob(os.path.join("..", "data", "caja*.yaml"))

if len(archivos_yamls) == 0:
    print("No se encontraron archivos YAML en el directorio especificado.")
    sys.exit(1)

# Crear una lista de DataFrames a partir de los archivos YAML encontrados
dataframes = []
for ruta in archivos_yamls:
    datos_yaml = funciones.leer_archivo_yml(ruta)
    df = funciones.convertir_a_dataframe(datos_yaml, spark)
    dataframes.append(df)

# Unir todos los DataFrames en uno solo
df_final = dataframes[0]
for df in dataframes[1:]:
    df_final = df_final.union(df)

# Crear la columna total_venta (cantidad * precio_unitario)
df_final = df_final.withColumn("total_venta", F.col("cantidad") * F.col("precio_unitario"))

# Calcular el total de productos vendidos
total_productos = df_final.groupBy("nombre_producto").agg(F.sum("cantidad").alias("cantidad_total"))

# Mostrar el total de productos vendidos en el CMD
print("\n--- Total de productos vendidos ---")
total_productos.show()

# Calcular el total de ventas por caja
total_cajas = df_final.groupBy("numero_caja").agg(F.sum("total_venta").alias("total_vendido"))

# Mostrar el total vendido por caja en el CMD
print("\n--- Total vendido por caja ---")
total_cajas.show()

# Calcular las métricas, incluyendo la fecha
caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75 = funciones.calcular_metricas_con_fecha(df_final)
producto_mas_vendido, producto_mayor_ingreso = funciones.calcular_productos(df_final)

# Extraer la fecha de las compras (opcional)
fecha = df_final.select(F.first("fecha")).first()["first(fecha)"]

# Crear un DataFrame para las métricas con la fecha incluida
metricas_data = [
    ("caja_con_mas_ventas", caja_con_mas_ventas, fecha),
    ("caja_con_menos_ventas", caja_con_menos_ventas, fecha),
    ("percentil_25_por_caja", percentil_25, fecha),
    ("percentil_50_por_caja", percentil_50, fecha),
    ("percentil_75_por_caja", percentil_75, fecha),
    ("producto_mas_vendido_por_unidad", producto_mas_vendido, fecha),
    ("producto_de_mayor_ingreso", producto_mayor_ingreso, fecha)
]

# Definir el esquema sin la tilde
schema_metricas = "Metrica STRING, Valor STRING, Fecha STRING"

df_metricas = spark.createDataFrame(metricas_data, schema=schema_metricas)

# Mostrar las métricas como una tabla en el CMD
print("\n--- Métricas con fecha ---")
df_metricas.show()

# Función para eliminar la carpeta si ya existe
def eliminar_carpeta_si_existe(ruta_carpeta):
    if os.path.exists(ruta_carpeta):
        shutil.rmtree(ruta_carpeta)

# Eliminar las carpetas si ya existen para sobrescribir
eliminar_carpeta_si_existe("/src/output/total_productos")
eliminar_carpeta_si_existe("/src/output/total_cajas")
eliminar_carpeta_si_existe("/src/output/metricas")

# Guardar los resultados de total_productos en una carpeta y un archivo
total_productos.coalesce(1).write.mode("overwrite").csv("/src/output/total_productos", header=True)

# Guardar los resultados de total_cajas en una carpeta y un archivo
total_cajas.coalesce(1).write.mode("overwrite").csv("/src/output/total_cajas", header=True)

# Guardar las métricas con la fecha en la carpeta "metricas" con un solo archivo
df_metricas.coalesce(1).write.mode("overwrite").csv("/src/output/metricas", header=True)

# Finalizar la sesión de Spark
spark.stop()
