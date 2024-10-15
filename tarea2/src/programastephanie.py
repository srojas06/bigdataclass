import yaml
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Crear la sesión de Spark
spark = SparkSession.builder.appName("Tarea2BigData").getOrCreate()

# Función para leer archivos YAML
def leer_archivo_yml(ruta):
    with open(ruta, 'r') as archivo:
        return yaml.safe_load(archivo)

# Función para convertir los datos YAML a un DataFrame de Spark
def convertir_a_dataframe(dato_yaml):
    compras = []
    for compra in dato_yaml[1]["- compras"]:
        for producto in compra["- compra"]:
            compras.append({
                "numero_caja": dato_yaml[0]["- numero_caja"],
                "nombre_producto": producto["- nombre"],
                "cantidad": producto["cantidad"],
                "precio_unitario": producto["precio_unitario"]
            })
    return spark.createDataFrame(compras)

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
    datos_yaml = leer_archivo_yml(ruta)
    df = convertir_a_dataframe(datos_yaml)
    dataframes.append(df)

# Unir todos los DataFrames en uno solo
df_final = dataframes[0]
for df in dataframes[1:]:
    df_final = df_final.union(df)

# Calcular el total vendido por producto
total_productos = df_final.groupBy("nombre_producto").agg(F.sum("cantidad").alias("total_vendido"))
total_productos.show()

# Calcular el total de ventas por caja
df_final = df_final.withColumn("total_venta", F.col("cantidad") * F.col("precio_unitario"))
total_cajas = df_final.groupBy("numero_caja").agg(F.sum("total_venta").alias("total_vendido"))
total_cajas.show()

# Calcular las métricas de percentiles
percentiles = df_final.selectExpr(
    "percentile_approx(total_venta, 0.25) as percentil_25",
    "percentile_approx(total_venta, 0.50) as percentil_50",
    "percentile_approx(total_venta, 0.75) as percentil_75"
)
percentiles.show()

# Guardar los resultados en archivos CSV
total_productos.write.csv("/src/output/total_productos.csv", header=True)
total_cajas.write.csv("/src/output/total_cajas.csv", header=True)
percentiles.write.csv("/src/output/metricas.csv", header=True)

# Finalizar la sesión de Spark
spark.stop()

