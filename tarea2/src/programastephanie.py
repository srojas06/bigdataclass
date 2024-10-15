from pyspark.sql import SparkSession
import funciones 

# creamos la sesión de spark
spark = SparkSession.builder.appName("Tarea2BigData").getOrCreate()

# rutas específicas para los archivos YAML
archivos_yamls = [
    "/src/data/caja1.yaml",
    "/src/data/caja2.yaml",
    "/src/data/caja3.yaml",
    "/src/data/caja4.yaml",
    "/src/data/caja5.yaml"
]

# creamos una lista de dataFrames
dataframes = []
for ruta in archivos_yamls:
    datos_yaml = funciones.leer_archivo_yml(ruta)
    df = funciones.convertir_a_dataframe(datos_yaml, spark)
    dataframes.append(df)

#unimos los dataframes
df_final = dataframes[0]
for df in dataframes[1:]:
    df_final = df_final.union(df)

# calculamos las métricas
caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75 = funciones.calcular_metricas(df_final)
producto_mas_vendido, producto_mayor_ingreso = funciones.calcular_productos(df_final)

# mostramos los datos en el cmd
print(f"Caja con más ventas: {caja_con_mas_ventas}")
print(f"Caja con menos ventas: {caja_con_menos_ventas}")
print(f"Percentil 25: {percentil_25}")
print(f"Percentil 50 (Mediana): {percentil_50}")
print(f"Percentil 75: {percentil_75}")
print(f"Producto más vendido: {producto_mas_vendido}")
print(f"Producto de mayor ingreso: {producto_mayor_ingreso}")

# guardamos los resultadoos  en CSV, usamos "overwrite" para sobrescribir los archivos existentes
df_final.write.mode("overwrite").csv("/src/output/total_productos.csv", header=True)
df_final.write.mode("overwrite").csv("/src/output/total_cajas.csv", header=True)

# guardamos las métricas
from pyspark.sql import Row
metricas = [
    Row(metrica="caja_con_mas_ventas", valor=caja_con_mas_ventas),
    Row(metrica="caja_con_menos_ventas", valor=caja_con_menos_ventas),
    Row(metrica="percentil_25_por_caja", valor=percentil_25),
    Row(metrica="percentil_50_por_caja", valor=percentil_50),
    Row(metrica="percentil_75_por_caja", valor=percentil_75),
    Row(metrica="producto_mas_vendido_por_unidad", valor=producto_mas_vendido),
    Row(metrica="producto_de_mayor_ingreso", valor=producto_mayor_ingreso)
]

df_metricas = spark.createDataFrame(metricas)
df_metricas.write.mode("overwrite").csv("/src/output/metricas.csv", header=True)

spark.stop()

