from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import funciones  # Importar las funciones desde el archivo funciones.py

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Tarea2BigData").getOrCreate()

# Lista de rutas específicas para los archivos YAML
archivos_yamls = [
    "/src/data/caja1.yaml",
    "/src/data/caja2.yaml",
    "/src/data/caja3.yaml",
    "/src/data/caja4.yaml",
    "/src/data/caja5.yaml"
]

# Crear una lista de DataFrames
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

# Calcular las métricas
caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75 = funciones.calcular_metricas(df_final)
producto_mas_vendido, producto_mayor_ingreso = funciones.calcular_productos(df_final)

# Mostrar las métricas en el CMD
print("\n--- Métricas ---")
print(f"Caja con más ventas: {caja_con_mas_ventas}")
print(f"Caja con menos ventas: {caja_con_menos_ventas}")
print(f"Percentil 25: {percentil_25}")
print(f"Percentil 50 (Mediana): {percentil_50}")
print(f"Percentil 75: {percentil_75}")
print(f"Producto más vendido: {producto_mas_vendido}")
print(f"Producto de mayor ingreso: {producto_mayor_ingreso}")

# Guardar los resultados de total_productos en una carpeta y un archivo
total_productos.coalesce(1).write.mode("overwrite").csv("/src/output/total_productos", header=True)

# Guardar los resultados de total_cajas en una carpeta y un archivo
total_cajas.coalesce(1).write.mode("overwrite").csv("/src/output/total_cajas", header=True)

# Guardar las métricas en la carpeta "metricas" con un solo archivo
funciones.guardar_metricas(caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75, producto_mas_vendido, producto_mayor_ingreso, spark)

# Finalizar la sesión de Spark
spark.stop()
