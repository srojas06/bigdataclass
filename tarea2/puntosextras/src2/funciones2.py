import yaml
from pyspark.sql import SparkSession, Row
import pyspark.sql.functions as F

# Función para leer archivo YAML
def leer_archivo_yml(ruta_archivo):
    with open(ruta_archivo, 'r') as file:
        datos = yaml.safe_load(file)
    return datos

# Función para convertir el archivo YAML a un DataFrame de Spark
def convertir_a_dataframe(datos_yaml, spark):
    compras_list = []

    for dato_yaml in datos_yaml:
        numero_caja = dato_yaml.get('- numero_caja', None)
        compras = dato_yaml.get('- compras', [])

        # Verificar si la lista de compras está vacía
        if not compras:
            continue

        for compra in compras:
            productos = compra.get('- compra', [])

            for producto in productos:
                nombre = producto.get('- nombre', None)
                cantidad = producto.get('cantidad', 0)
                precio_unitario = producto.get('precio_unitario', 0)
                fecha = producto.get('fecha', None)  # Fecha es opcional

                # Agregar datos a la lista de compras
                compras_list.append({
                    'numero_caja': numero_caja,
                    'nombre': nombre,
                    'cantidad': cantidad,
                    'precio_unitario': precio_unitario,
                    'fecha': fecha
                })

    # Verificar si la lista tiene datos antes de crear el DataFrame
    if not compras_list:
        raise ValueError("No se encontraron datos para crear el DataFrame.")

    # Crear el DataFrame de Spark
    df_spark = spark.createDataFrame([Row(**compra) for compra in compras_list])
    return df_spark

# Función para calcular métricas
def calcular_metricas(df):
    caja_con_mas_ventas = df.groupBy('numero_caja') \
        .agg(F.sum('total_venta').alias('total_ventas')) \
        .orderBy(F.desc('total_ventas')) \
        .first()['numero_caja']

    caja_con_menos_ventas = df.groupBy('numero_caja') \
        .agg(F.sum('total_venta').alias('total_ventas')) \
        .orderBy(F.asc('total_ventas')) \
        .first()['numero_caja']

    percentiles = df.approxQuantile('total_venta', [0.25, 0.5, 0.75], 0.01)
    percentil_25, percentil_50, percentil_75 = percentiles

    return caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75

# Función para calcular métricas de productos
def calcular_productos(df):
    producto_mas_vendido = df.groupBy('nombre') \
        .agg(F.sum('cantidad').alias('total_cantidad')) \
        .orderBy(F.desc('total_cantidad')) \
        .first()['nombre']

    producto_mayor_ingreso = df.withColumn('ingreso', F.col('cantidad') * F.col('precio_unitario')) \
        .groupBy('nombre') \
        .agg(F.sum('ingreso').alias('total_ingreso')) \
        .orderBy(F.desc('total_ingreso')) \
        .first()['nombre']

    return producto_mas_vendido, producto_mayor_ingreso
