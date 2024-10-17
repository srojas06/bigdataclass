import yaml
from pyspark.sql import Row

def leer_archivo_yml(ruta):
    with open(ruta, 'r') as archivo:
        try:
            return yaml.safe_load(archivo)
        except yaml.YAMLError as error:
            print(f"Error al leer el archivo YAML: {error}")
            return None

def convertir_a_dataframe(datos_yaml, spark):
    if not datos_yaml or 'compras' not in datos_yaml:
        raise ValueError("El archivo YAML no contiene la estructura correcta. Verifique el contenido.")

    compras_list = []

    # Procesar los datos YAML
    for compra in datos_yaml['compras']:
        productos = compra.get('compra', [])
        for producto in productos:
            producto_data = producto.get('producto', {})
            if producto_data:  # Solo añadir si hay datos de producto
                compras_list.append(
                    Row(
                        nombre=producto_data.get('nombre'),
                        cantidad=producto_data.get('cantidad'),
                        precio_unitario=producto_data.get('precio_unitario'),
                        fecha=producto_data.get('fecha', None)
                    )
                )

    # Debug: Verificar el contenido de la lista de compras
    if not compras_list:
        raise ValueError("No se encontraron datos para crear el DataFrame.")

    # Crear el DataFrame
    df_spark = spark.createDataFrame(compras_list)

    return df_spark

def calcular_metricas(df):
    # Calcular caja con más ventas y caja con menos ventas
    df_cajas = df.groupBy('nombre').agg(F.sum('total_venta').alias('total_vendido'))
    caja_con_mas_ventas = df_cajas.orderBy(F.desc('total_vendido')).first()['nombre']
    caja_con_menos_ventas = df_cajas.orderBy('total_vendido').first()['nombre']

    # Calcular percentiles
    percentil_25 = df.approxQuantile('total_venta', [0.25], 0.01)[0]
    percentil_50 = df.approxQuantile('total_venta', [0.50], 0.01)[0]
    percentil_75 = df.approxQuantile('total_venta', [0.75], 0.01)[0]

    return caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75

def calcular_productos(df):
    # Calcular el producto más vendido por unidad
    df_productos = df.groupBy('nombre').agg(F.sum('cantidad').alias('cantidad_total'))
    producto_mas_vendido = df_productos.orderBy(F.desc('cantidad_total')).first()['nombre']

    # Calcular el producto que generó más ingresos
    df_ingresos = df.groupBy('nombre').agg(F.sum('total_venta').alias('ingreso_total'))
    producto_mayor_ingreso = df_ingresos.orderBy(F.desc('ingreso_total')).first()['nombre']

    return producto_mas_vendido, producto_mayor_ingreso
