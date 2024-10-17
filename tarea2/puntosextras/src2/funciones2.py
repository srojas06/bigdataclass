import yaml
from pyspark.sql import Row
from pyspark.sql import functions as F

def leer_archivo_yml(ruta):
    with open(ruta, 'r') as archivo:
        try:
            return yaml.safe_load(archivo)
        except yaml.YAMLError as error:
            print(f"Error al leer el archivo YAML: {error}")
            return None

def convertir_a_dataframe(datos_yaml, spark):
    # Verificar que el YAML contenga la estructura correcta
    if not datos_yaml or not isinstance(datos_yaml, list):
        raise ValueError("El archivo YAML no contiene la estructura correcta. Verifique el contenido.")

    compras_list = []

    # Procesar la estructura del archivo YAML para crear la lista de Rows de Spark
    for caja in datos_yaml:
        numero_caja = caja.get("numero_caja")
        compras = caja.get("compras", [])

        for compra in compras:
            productos = compra.get("compra", [])
            for producto in productos:
                producto_data = producto.get("producto", {})
                if producto_data:
                    compras_list.append(
                        Row(
                            numero_caja=numero_caja,
                            nombre=producto_data.get('nombre'),
                            cantidad=producto_data.get('cantidad'),
                            precio_unitario=producto_data.get('precio_unitario'),
                            fecha=producto_data.get('fecha', None)
                        )
                    )

    # Verificar si la lista de compras está vacía, y en ese caso lanzar un error
    if not compras_list:
        raise ValueError("No se encontraron datos para crear el DataFrame.")

    # Crear el DataFrame de Spark a partir de la lista de Rows
    df_spark = spark.createDataFrame(compras_list)

    return df_spark

def calcular_metricas(df):
    # Calcular caja con más ventas y caja con menos ventas
    df_cajas = df.groupBy('numero_caja').agg(F.sum('cantidad' * 'precio_unitario').alias('total_vendido'))
    caja_con_mas_ventas = df_cajas.orderBy(F.desc('total_vendido')).first()['numero_caja']
    caja_con_menos_ventas = df_cajas.orderBy('total_vendido').first()['numero_caja']

    # Calcular percentiles
    percentil_25 = df.approxQuantile('precio_unitario', [0.25], 0.01)[0]
    percentil_50 = df.approxQuantile('precio_unitario', [0.50], 0.01)[0]
    percentil_75 = df.approxQuantile('precio_unitario', [0.75], 0.01)[0]

    return caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75

def calcular_productos(df):
    # Calcular el producto más vendido por unidad
    df_productos = df.groupBy('nombre').agg(F.sum('cantidad').alias('cantidad_total'))
    producto_mas_vendido = df_productos.orderBy(F.desc('cantidad_total')).first()['nombre']

    # Calcular el producto que generó más ingresos
    df_ingresos = df.groupBy('nombre').agg(F.sum(df['cantidad'] * df['precio_unitario']).alias('ingreso_total'))
    producto_mayor_ingreso = df_ingresos.orderBy(F.desc('ingreso_total')).first()['nombre']

    return producto_mas_vendido, producto_mayor_ingreso

