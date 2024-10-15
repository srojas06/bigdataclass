from pyspark.sql import SparkSession
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

# Calcular las métricas
caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75 = funciones.calcular_metricas(df_final)
producto_mas_vendido, producto_mayor_ingreso = funciones.calcular_productos(df_final)

# Mostrar los resultados en la terminal (CMD)
print(f"Caja con más ventas: {caja_con_mas_ventas}")
print(f"Caja con menos ventas: {caja_con_menos_ventas}")
print(f"Percentil 25: {percentil_25}")
print(f"Percentil 50 (Mediana): {percentil_50}")
print(f"Percentil 75: {percentil_75}")
print(f"Producto más vendido: {producto_mas_vendido}")
print(f"Producto de mayor ingreso: {producto_mayor_ingreso}")

# Guardar los resultados en CSV, usando "overwrite" para sobrescribir los archivos existentes
df_final.write.mode("overwrite").csv("/src/output/total_productos.csv", header=True)
df_final.write.mode("overwrite").csv("/src/output/total_cajas.csv", header=True)

# Guardar las métricas
funciones.guardar_metricas(caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75, producto_mas_vendido, producto_mayor_ingreso, spark)

# Finalizar la sesión de Spark
spark.stop()

