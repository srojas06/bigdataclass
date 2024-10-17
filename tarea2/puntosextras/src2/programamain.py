import sys
import funciones2  # Importar las funciones desde funciones2.py
import psycopg2
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Crear la sesión de Spark
spark = SparkSession.builder.appName("PuntosExtrasBigData").getOrCreate()

# Deshabilitar los logs innecesarios de Spark
spark.sparkContext.setLogLevel("ERROR")

# Verificar si el archivo YAML se ha proporcionado como argumento
if len(sys.argv) < 7:
    print("Uso: programamain.py <ruta_archivo_yaml1> <ruta_archivo_yaml2> ... <host> <usuario> <password> <nombre_bd>")
    sys.exit(1)

# Extraer argumentos de archivos YAML y conexión
rutas_archivos_yaml = sys.argv[1:-4]
host = sys.argv[-4]
usuario = sys.argv[-3]
password = sys.argv[-2]
nombre_bd = sys.argv[-1]

# Leer y combinar los archivos YAML
try:
    datos_yaml = funciones2.leer_y_combinar_archivos_yaml(rutas_archivos_yaml, spark)
except ValueError as e:
    print(f"Error al leer y combinar archivos YAML: {e}")
    sys.exit(1)

# Mostrar el DataFrame creado desde YAML
print("\n--- DataFrame creado desde YAML combinado ---")
datos_yaml.show()

# Crear la columna total_venta (cantidad * precio_unitario)
df = datos_yaml.withColumn("total_venta", F.col("cantidad") * F.col("precio_unitario"))

# Mostrar el DataFrame con la columna total_venta
print("\n--- DataFrame con columna total_venta ---")
df.show()

# Calcular las métricas
print("\n--- Calculando métricas ---")
caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75 = funciones2.calcular_metricas(df)
producto_mas_vendido, producto_mayor_ingreso = funciones2.calcular_productos(df)

# Crear un DataFrame para las métricas con la fecha incluida (si está disponible)
fecha = df.select(F.first(F.col("fecha"), ignorenulls=True)).first()[0] if 'fecha' in df.columns else None
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
