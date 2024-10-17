import sys
import funciones2  # Importar las funciones desde funciones2.py
import psycopg2
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Crear la sesión de Spark
spark = SparkSession.builder.appName("PuntosExtrasBigData").getOrCreate()

# Deshabilitar los logs innecesarios de Spark
spark.sparkContext.setLogLevel("ERROR")

# Verificar si los argumentos se han proporcionado
if len(sys.argv) < 6:
    print("Uso: programamain.py <ruta_archivo_yaml_1> [<ruta_archivo_yaml_2> ...] <host> <usuario> <password> <nombre_bd>")
    sys.exit(1)

# Extraer los argumentos de conexión a la base de datos
host = sys.argv[-4]
usuario = sys.argv[-3]
password = sys.argv[-2]
nombre_bd = sys.argv[-1]

# Extraer las rutas de los archivos YAML (todos los argumentos hasta antes de los parámetros de conexión)
rutas_archivos_yaml = sys.argv[1:-4]

# Crear un DataFrame vacío para consolidar los datos de todos los archivos
df_total = None

# Procesar cada archivo YAML
for ruta_archivo_yaml in rutas_archivos_yaml:
    # Leer el archivo YAML
    datos_yaml = funciones2.leer_archivo_yml(ruta_archivo_yaml)

    # Verificar si hubo un problema al leer el archivo YAML
    if datos_yaml is None:
        print(f"Error al leer el archivo YAML: {ruta_archivo_yaml}. Por favor, verifica el contenido.")
        continue

    # Mostrar los datos leídos para depuración
    print(f"\n--- Datos leídos del archivo YAML: {ruta_archivo_yaml} ---")
    print(datos_yaml)

    # Convertir los datos a DataFrame de Spark
    try:
        df = funciones2.convertir_a_dataframe(datos_yaml, spark)
    except ValueError as e:
        print(f"Error: {e}")
        continue

    # Añadir el DataFrame actual al DataFrame consolidado
    if df_total is None:
        df_total = df
    else:
        df_total = df_total.union(df)

# Verificar si algún archivo fue procesado con éxito
if df_total is None:
    print("No se procesaron archivos YAML con éxito.")
    sys.exit(1)

# Crear la columna total_venta (cantidad * precio_unitario)
df_total = df_total.withColumn("total_venta", F.col("cantidad") * F.col("precio_unitario"))

# Mostrar el DataFrame con la columna total_venta
print("\n--- DataFrame con columna total_venta ---")
df_total.show()

# Calcular las métricas
print("\n--- Calculando métricas ---")
caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75 = funciones2.calcular_metricas(df_total)
producto_mas_vendido, producto_mayor_ingreso = funciones2.calcular_productos(df_total)

# Crear un DataFrame para las métricas con la fecha incluida (si está disponible)
fecha = df_total.select(F.first(F.col("fecha"), ignorenulls=True)).first()[0] if 'fecha' in df_total.columns else None
metricas_data = [
    ("caja_con_mas_ventas", caja_con_mas_ventas, fecha),
    ("caja_con_menos_ventas", caja_con_menos_ventas, fecha),
    ("percentil_25_por_caja", percentil_25, fecha),
    ("percentil_50_por_caja", percentil_50, fecha),
    ("percentil_75_por_caja", percentil_75, fecha),
    ("producto_mas_vendido_por_unidad", producto_mas_vendido, fecha),
    ("producto_de_mayor_ingreso", producto_mayor_ingreso, fecha)
]

# Definir el esquema del DataFrame de métricas
schema_metricas = "Metrica STRING, Valor STRING, Fecha STRING"
df_metricas = spark.createDataFrame(metricas_data, schema=schema_metricas)

# Mostrar las métricas como una tabla en el CMD
print("\n--- Métricas con fecha ---")
df_metricas.show()

# Conectar a la base de datos PostgreSQL y crear la tabla e insertar los datos
conexion = None
try:
    conexion = psycopg2.connect(
        host=host,
        database=nombre_bd,
        user=usuario,
        password=password
    )
    cursor = conexion.cursor()

    # Crear la tabla si no existe
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS metricas (
            id SERIAL PRIMARY KEY,
            metrica VARCHAR(255),
            valor VARCHAR(255),
            fecha VARCHAR(255)
        )
    ''')

    # Insertar los datos de las métricas en la tabla
    for row in metricas_data:
        cursor.execute(
            "INSERT INTO metricas (metrica, valor, fecha) VALUES (%s, %s, %s)",
            (row[0], str(row[1]), row[2])
        )

    # Confirmar los cambios
    conexion.commit()

except (Exception, psycopg2.Error) as error:
    print("Error al conectar a la base de datos PostgreSQL", error)

finally:
    # Cerrar la conexión a la base de datos si fue exitosa
    if conexion is not None:
        cursor.close()
        conexion.close()
        print("Conexión a PostgreSQL cerrada")

# Finalizar la sesión de Spark
spark.stop()



