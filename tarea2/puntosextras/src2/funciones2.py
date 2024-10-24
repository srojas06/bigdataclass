import yaml
from pyspark.sql import Row
import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from pyspark.sql.types import DoubleType

# Función para leer el archivo YAML
def leer_archivo_yml(ruta):
    with open(ruta, 'r') as archivo:
        try:
            data = yaml.safe_load(archivo)
            print("\n--- Datos leídos del archivo YAML desde: {} ---".format(ruta))
            print(data)
            return data
        except yaml.YAMLError as error:
            print(f"Error al leer el archivo YAML: {error}")
            return None

# función para leer y combinar múltiples archivos YAML
def leer_y_combinar_archivos_yaml(rutas, spark):
    dfs = []
    for ruta in rutas:
        datos_yaml = leer_archivo_yml(ruta)
        if datos_yaml is not None:
            df = convertir_a_dataframe(datos_yaml, spark)
            dfs.append(df)

    if not dfs:
        raise ValueError("No se pudo crear ningún DataFrame a partir de los archivos YAML proporcionados.")

    # Unir todos los DataFrames
    df_total = dfs[0]
    for df in dfs[1:]:
        df_total = df_total.union(df)

    print("\n--- DataFrame después de la unión de todos los DataFrames ---")
    df_total.show(truncate=False, n=1000)

    # Eliminar duplicados después de unir todos los DataFrames
    df_total = df_total.dropDuplicates()

    print("\n--- DataFrame después de eliminar duplicados ---")
    df_total.show(truncate=False, n=1000)

    return df_total

# Función para convertir los datos YAML en un DataFrame de Spark
def convertir_a_dataframe(datos_yaml, spark):
    if not datos_yaml or not isinstance(datos_yaml, dict) or 'numero_caja' not in datos_yaml or 'compras' not in datos_yaml:
        raise ValueError("El archivo YAML no contiene la estructura correcta. Verifique el contenido.")

    numero_caja = datos_yaml['numero_caja']
    compras_list = []

    # Procesa las compras
    for compra in datos_yaml['compras']:
        productos = compra.get('compra', [])
        if isinstance(productos, list):
            for producto in productos:
                if 'producto' in producto and 'nombre' in producto['producto'] and 'cantidad' in producto['producto'] and 'precio_unitario' in producto['producto']:
                    compras_list.append(
                        Row(
                            numero_caja=numero_caja,
                            nombre=producto['producto']['nombre'],
                            cantidad=producto['producto']['cantidad'],
                            precio_unitario=producto['producto']['precio_unitario'],
                            fecha=producto['producto'].get('fecha', None)  # Si no tiene fecha, se deja como None
                        )
                    )
                else:
                    print(f"Producto no válido encontrado: {producto}")

    # Verificamos si la lista de compras tiene datos
    if not compras_list:
        raise ValueError("No se encontraron datos para crear el DataFrame.")

    # Creamos el DataFrame de Spark
    df_spark = spark.createDataFrame(compras_list)
    print("\n--- DataFrame creado desde YAML ---")
    df_spark.show(truncate=False, n=1000)  

    return df_spark

# Función para calcular métricas sobre el DataFrame
def calcular_metricas(df):
    # Crea la columna 'total_venta' (cantidad * precio_unitario)
    df = df.withColumn('total_venta', F.col('cantidad') * F.col('precio_unitario'))
    print("\n--- DataFrame con columna total_venta ---")
    df.show(truncate=False, n=1000)  

    # Calcula caja con más ventas y caja con menos ventas
    df_cajas = df.groupBy('numero_caja').agg(F.sum('total_venta').alias('total_vendido'))
    print("\n--- Total vendido por caja ---")
    df_cajas.show(truncate=False, n=1000)

    # Ordena por la columna de 'total_vendido' para obtener las métricas
    caja_con_mas_ventas = df_cajas.orderBy(F.desc('total_vendido')).first()['numero_caja']
    caja_con_menos_ventas = df_cajas.orderBy('total_vendido').first()['numero_caja']

    # Calcula los percentilesss
    percentil_25 = df_cajas.approxQuantile('total_vendido', [0.25], 0.01)[0]
    percentil_50 = df_cajas.approxQuantile('total_vendido', [0.50], 0.01)[0]
    percentil_75 = df_cajas.approxQuantile('total_vendido', [0.75], 0.01)[0]

    print(f"Caja con más ventas: {caja_con_mas_ventas}")
    print(f"Caja con menos ventas: {caja_con_menos_ventas}")
    print(f"Percentil 25: {percentil_25}")
    print(f"Percentil 50: {percentil_50}")
    print(f"Percentil 75: {percentil_75}")

    return caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75

# Función para calcular métricas relacionadas a productos
def calcular_productos(df):
    # Calcula el producto más vendido por unidad
    df_productos = df.groupBy('nombre').agg(F.sum('cantidad').alias('cantidad_total'))
    print("\n--- Cantidad total vendida por producto ---")
    df_productos.show(truncate=False, n=1000)
    producto_mas_vendido = df_productos.orderBy(F.desc('cantidad_total')).first()['nombre']

    # Calcular el producto que generó más ingresos
    df_ingresos = df.groupBy('nombre').agg(F.sum(F.col('cantidad') * F.col('precio_unitario')).alias('ingreso_total'))
    print("\n--- Ingreso total por producto ---")
    df_ingresos.show(truncate=False, n=1000)
    producto_mayor_ingreso = df_ingresos.orderBy(F.desc('ingreso_total')).first()['nombre']

    print(f"Producto más vendido por unidad: {producto_mas_vendido}")
    print(f"Producto de mayor ingreso: {producto_mayor_ingreso}")

    return producto_mas_vendido, producto_mayor_ingreso

# Función para generar un DataFrame con métricas incluyendo la fecha
def generar_dataframe_metricas_con_fecha(metricas_data, spark):
    # Crea una lista de rows a partir de los datos de métricas
    rows = []
    for metrica, valor, fecha in metricas_data:
        # Asegura de que el valor sea del tipo correcto (string para métricas como 'leche' o cervezaaa)
        if isinstance(valor, (int, float)):
            valor = float(valor)
        else:
            valor = str(valor)
        rows.append(Row(metrica=metrica, valor=valor, fecha=fecha))

    # Crea DataFrame a partir de las filas
    df_metricas = spark.createDataFrame(rows)
    print("\n--- DataFrame de métricas con fecha ---")
    df_metricas.show(truncate=False, n=1000)  # Muestra el DataFrame completo

    return df_metricas
