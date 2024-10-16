import psycopg2
from pyspark.sql import functions as F
from pyspark.sql import Row

def conectar_base_de_datos():
    try:
        conn = psycopg2.connect(
            host="localhost",
            port="5433",
            database="postgres",
            user="postgres",
            password="testPassword"
        )
        print("Conexión exitosa a la base de datos.")
        return conn
    except Exception as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

def leer_archivo_yml(ruta):
    with open(ruta, 'r') as archivo:
        return yaml.safe_load(archivo)

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

def guardar_metricas_en_db(caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75, producto_mas_vendido, producto_mayor_ingreso):
    conn = conectar_base_de_datos()
    if conn is None:
        print("No se pudo conectar a la base de datos. Saliendo...")
        return

    try:
        cur = conn.cursor()
        cur.execute('''
            CREATE TABLE IF NOT EXISTS metricas (
                id SERIAL PRIMARY KEY,
                metrica VARCHAR(255),
                valor VARCHAR(255)
            )
        ''')
        metricas = [
            ("caja_con_mas_ventas", caja_con_mas_ventas),
            ("caja_con_menos_ventas", caja_con_menos_ventas),
            ("percentil_25_por_caja", percentil_25),
            ("percentil_50_por_caja", percentil_50),
            ("percentil_75_por_caja", percentil_75),
            ("producto_mas_vendido_por_unidad", producto_mas_vendido),
            ("producto_de_mayor_ingreso", producto_mayor_ingreso)
        ]
        cur.executemany("INSERT INTO metricas (metrica, valor) VALUES (%s, %s)", metricas)
        conn.commit()
        cur.close()
        print("Métricas guardadas en la base de datos exitosamente.")
    except Exception as e:
        print(f"Error al guardar las métricas en la base de datos: {e}")
    finally:
        conn.close()

def calcular_total_productos(df):
    # Convertir todos los nombres a minúsculas antes de hacer la agregación
    df = df.withColumn("nombre_producto", F.lower(F.col("nombre_producto")))
    
    # Filtrar productos con nombres o cantidades nulas, cantidades negativas o cero
    df_filtrado = df.filter(F.col("nombre_producto").isNotNull() & F.col("cantidad").isNotNull() & (F.col("cantidad") > 0))
    
    # Calcular el total de productos
    return df_filtrado.groupBy("nombre_producto").agg(F.sum("cantidad").alias("cantidad_total"))

# Calcular el total de ventas por caja (ignorando ventas negativas)
def calcular_total_cajas(df):
    # Convertir todas las ventas a DoubleType para evitar conflictos de tipos
    df = df.withColumn("total_venta", F.col("total_venta").cast("double"))
    
    # Filtrar cajas con identificador nulo
    df_filtrado = df.filter(F.col("numero_caja").isNotNull())
    
    # Filtrar devoluciones (ventas negativas)
    df_filtrado = df_filtrado.filter(F.col("total_venta") >= 0)
    
    # Calcular el total de ventas por caja
    return df_filtrado.groupBy("numero_caja").agg(F.sum("total_venta").alias("total_vendido"))
