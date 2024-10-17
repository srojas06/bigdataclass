import yaml
from pyspark.sql import Row

# Leer el archivo YAML
def leer_archivo_yml(ruta):
    with open(ruta, 'r') as stream:
        try:
            data = yaml.safe_load(stream)
            return data
        except yaml.YAMLError as exc:
            print(f"Error al leer el archivo YAML: {exc}")
            return None

# Función para convertir los datos YAML a un DataFrame de Spark
def convertir_a_dataframe(datos_yaml, spark):
    compras_list = []

    for item in datos_yaml:
        numero_caja = item.get('- numero_caja')
        compras = item.get('- compras', [])
        for compra in compras:
            productos = compra.get('- compra', [])
            for producto in productos:
                nombre = producto.get('- nombre')
                cantidad = producto.get('cantidad')
                precio_unitario = producto.get('precio_unitario')
                fecha = producto.get('fecha', None)  # Puede no estar presente

                compras_list.append(Row(
                    numero_caja=numero_caja,
                    nombre=nombre,
                    cantidad=cantidad,
                    precio_unitario=precio_unitario,
                    fecha=fecha
                ))

    # Convertir la lista de Row a un DataFrame de Spark
    df_spark = spark.createDataFrame(compras_list)
    return df_spark

# Calcular métricas (Ejemplo)
def calcular_metricas(df):
    # Calcular las métricas aquí (ejemplo simple)
    caja_con_mas_ventas = df.groupBy("numero_caja").sum("total_venta").orderBy("sum(total_venta)", ascending=False).first()["numero_caja"]
    caja_con_menos_ventas = df.groupBy("numero_caja").sum("total_venta").orderBy("sum(total_venta)", ascending=True).first()["numero_caja"]
    
    percentil_25 = df.approxQuantile("total_venta", [0.25], 0.0)[0]
    percentil_50 = df.approxQuantile("total_venta", [0.50], 0.0)[0]
    percentil_75 = df.approxQuantile("total_venta", [0.75], 0.0)[0]
    
    return caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75

def calcular_productos(df):
    # Calcular los productos más vendidos y con mayor ingreso
    producto_mas_vendido = df.groupBy("nombre").sum("cantidad").orderBy("sum(cantidad)", ascending=False).first()["nombre"]
    producto_mayor_ingreso = df.withColumn("ingreso_total", df.cantidad * df.precio_unitario).groupBy("nombre").sum("ingreso_total").orderBy("sum(ingreso_total)", ascending=False).first()["nombre"]
    
    return producto_mas_vendido, producto_mayor_ingreso
