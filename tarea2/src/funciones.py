import yaml
from pyspark.sql import functions as F
from pyspark.sql import Row

def leer_archivo_yml(ruta):
    with open(ruta, 'r') as archivo:
        return yaml.safe_load(archivo)

def convertir_a_dataframe(dato_yaml, spark):
    compras = []
    for compra in dato_yaml[1]["- compras"]:
        for producto in compra["- compra"]:
            compra_data = {
                "numero_caja": dato_yaml[0]["- numero_caja"],
                "nombre_producto": producto["- nombre"],
                "cantidad": producto["cantidad"],
                "precio_unitario": float(producto["precio_unitario"])
            }
            if "- fecha" in producto:
                compra_data["fecha"] = producto["- fecha"]
            else:
                compra_data["fecha"] = None
            compras.append(compra_data)

    schema = "numero_caja STRING, nombre_producto STRING, cantidad INT, precio_unitario DOUBLE, fecha STRING"
    return spark.createDataFrame(compras, schema)

# Mantener el nombre de la función como calcular_metricas
def calcular_metricas(df_final):
    caja_con_mas_ventas = df_final.groupBy("numero_caja").agg(F.sum("total_venta").alias("total_vendido")).orderBy(F.col("total_vendido").desc()).first()["numero_caja"]
    caja_con_menos_ventas = df_final.groupBy("numero_caja").agg(F.sum("total_venta").alias("total_vendido")).orderBy(F.col("total_vendido").asc()).first()["numero_caja"]
    percentil_25, percentil_50, percentil_75 = calcular_percentiles(df_final)
    return caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75

def calcular_percentiles(df_final):
    total_por_caja = df_final.groupBy("numero_caja").agg(F.sum("total_venta").alias("total_vendido"))
    percentil_25 = total_por_caja.selectExpr("percentile_approx(total_vendido, 0.25)").first()[0]
    percentil_50 = total_por_caja.selectExpr("percentile_approx(total_vendido, 0.50)").first()[0]
    percentil_75 = total_por_caja.selectExpr("percentile_approx(total_vendido, 0.75)").first()[0]
    return percentil_25, percentil_50, percentil_75

def calcular_productos(df_final):
    producto_mas_vendido = df_final.groupBy("nombre_producto").agg(F.sum("cantidad").alias("total_vendido")).orderBy(F.col("total_vendido").desc()).first()["nombre_producto"]
    producto_mayor_ingreso = df_final.groupBy("nombre_producto").agg(F.sum(F.col("cantidad") * F.col("precio_unitario")).alias("total_ingresos")).orderBy(F.col("total_ingresos").desc()).first()["nombre_producto"]
    return producto_mas_vendido, producto_mayor_ingreso

def guardar_metricas(caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75, producto_mas_vendido, producto_mayor_ingreso, fecha, spark):
    metricas = [
        Row(metrica="caja_con_mas_ventas", valor=caja_con_mas_ventas, fecha=fecha),
        Row(metrica="caja_con_menos_ventas", valor=caja_con_menos_ventas, fecha=fecha),
        Row(metrica="percentil_25_por_caja", valor=percentil_25, fecha=fecha),
        Row(metrica="percentil_50_por_caja", valor=percentil_50, fecha=fecha),
        Row(metrica="percentil_75_por_caja", valor=percentil_75, fecha=fecha),
        Row(metrica="producto_mas_vendido_por_unidad", valor=producto_mas_vendido, fecha=fecha),
        Row(metrica="producto_de_mayor_ingreso", valor=producto_mayor_ingreso, fecha=fecha)
    ]
    df_metricas = spark.createDataFrame(metricas)
    df_metricas.coalesce(1).write.mode("overwrite").csv("/src/output/metricas", header=True)

def calcular_total_productos(df):
    df = df.withColumn("nombre_producto", F.lower(F.col("nombre_producto")))
    df_filtrado = df.filter(F.col("nombre_producto").isNotNull() & F.col("cantidad").isNotNull() & (F.col("cantidad") > 0))
    return df_filtrado.groupBy("nombre_producto").agg(F.sum("cantidad").alias("cantidad_total"))

def calcular_total_cajas(df):
    df = df.withColumn("total_venta", F.col("total_venta").cast("double"))
    df_filtrado = df.filter(F.col("numero_caja").isNotNull())
    df_filtrado = df_filtrado.filter(F.col("total_venta") >= 0)
    return df_filtrado.groupBy("numero_caja").agg(F.sum("total_venta").alias("total_vendido"))
