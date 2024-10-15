import yaml
from pyspark.sql import functions as F
from pyspark.sql import Row

# Función para leer un archivo YAML y convertirlo a datos en formato dict
def leer_archivo_yml(ruta):
    with open(ruta, 'r') as archivo:
        return yaml.safe_load(archivo)

# Función para convertir el dict de YAML en un DataFrame de Spark
def convertir_a_dataframe(dato_yaml, spark):
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

# Función para calcular las métricas de las ventas por caja
def calcular_metricas(df_final):
    # Caja con más ventas
    caja_con_mas_ventas = df_final.groupBy("numero_caja").agg(F.sum("total_venta").alias("total_vendido")).orderBy(F.col("total_vendido").desc()).first()["numero_caja"]
    # Caja con menos ventas
    caja_con_menos_ventas = df_final.groupBy("numero_caja").agg(F.sum("total_venta").alias("total_vendido")).orderBy(F.col("total_vendido").asc()).first()["numero_caja"]

    # Percentiles 25, 50 y 75
    percentil_25, percentil_50, percentil_75 = calcular_percentiles(df_final)

    return caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75

# Función para calcular los percentiles de las ventas
def calcular_percentiles(df_final):
    total_por_caja = df_final.groupBy("numero_caja").agg(F.sum("total_venta").alias("total_vendido"))

    percentil_25 = total_por_caja.selectExpr("percentile_approx(total_vendido, 0.25)").first()[0]
    percentil_50 = total_por_caja.selectExpr("percentile_approx(total_vendido, 0.50)").first()[0]
    percentil_75 = total_por_caja.selectExpr("percentile_approx(total_vendido, 0.75)").first()[0]
    
    return percentil_25, percentil_50, percentil_75

# Función para calcular el producto más vendido y el de mayor ingreso
def calcular_productos(df_final):
    # Producto más vendido
    producto_mas_vendido = df_final.groupBy("nombre_producto").agg(F.sum("cantidad").alias("total_vendido")).orderBy(F.col("total_vendido").desc()).first()["nombre_producto"]
    # Producto que generó más ingresos
    producto_mayor_ingreso = df_final.groupBy("nombre_producto").agg(F.sum(F.col("cantidad") * F.col("precio_unitario")).alias("total_ingresos")).orderBy(F.col("total_ingresos").desc()).first()["nombre_producto"]

    return producto_mas_vendido, producto_mayor_ingreso

# Función para guardar las métricas en CSV
def guardar_metricas(caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75, producto_mas_vendido, producto_mayor_ingreso, spark):
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

    # Guardar en una carpeta llamada "metricas" con un solo archivo
    df_metricas.coalesce(1).write.mode("overwrite").csv("/src/output/metricas", header=True)