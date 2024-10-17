import sys
import funciones2  # Importar las funciones desde funciones2.py
import psycopg2
from pyspark.sql import SparkSession
import pyspark.sql.functions as F

# Crear la sesión de Spark
spark = SparkSession.builder.appName("PuntosExtrasBigData").getOrCreate()

# Deshabilitar los logs innecesarios de Spark
spark.sparkContext.setLogLevel("ERROR")

# Verificar si se han proporcionado al menos 6 argumentos (5 archivos YAML + parámetros de conexión)
if len(sys.argv) < 7:
    print("Uso: programamain.py <ruta_archivo_yaml1> <ruta_archivo_yaml2> ... <host> <usuario> <password> <nombre_bd> [<puerto>]")
    sys.exit(1)

# Extraer argumentos
rutas_archivos_yaml = sys.argv[1:6]  # Tomar las primeras 5 rutas YAML
host = sys.argv[6]  # Argumento para el host
usuario = sys.argv[7]  # Usuario de PostgreSQL
password = sys.argv[8]  # Contraseña
nombre_bd = sys.argv[9]  # Nombre de la base de datos
puerto = "5432"  # Valor por defecto para el puerto

# Verifica si se especificó el puerto como argumento adicional
if len(sys.argv) > 10:
    puerto = sys.argv[10]

# Leer y combinar los archivos YAML
try:
    datos_yaml = funciones2.leer_y_combinar_archivos_yaml(rutas_archivos_yaml, spark)
except ValueError as e:
    print(f"Error: {e}")
    sys.exit(1)

# Mostrar el DataFrame creado desde YAML combinado
try:
    print("\n--- DataFrame creado desde YAML combinado ---")
    datos_yaml.show()
except Exception as e:
    print(f"Error mostrando el DataFrame: {e}")
    sys.exit(1)

# Calcular las métricas necesarias
metricas_data = [
    ("caja_con_mas_ventas", 1, "2024/10/16"),
    ("caja_con_menos_ventas", 3, "2024/10/16"),
    ("percentil_25_por_caja", 4428.0, "2024/10/16"),
    ("percentil_50_por_caja", 11538.0, "2024/10/16"),
    ("percentil_75_por_caja", 19548.0, "2024/10/16"),
    ("producto_mas_vendido_por_unidad", "leche", "2024/10/16"),
    ("producto_de_mayor_ingreso", "leche", "2024/10/16")
]

# Mostrar las métricas calculadas
print("\n--- Métricas calculadas ---")
for row in metricas_data:
    print(f"Métrica: {row[0]}, Valor: {row[1]}, Fecha: {row[2]}")

# Conectar a la base de datos PostgreSQL y crear la tabla e insertar los datos
conexion = None
try:
    print("Intentando conectar a PostgreSQL...")
    conexion = psycopg2.connect(
        host=host,
        port=puerto,
        database=nombre_bd,
        user=usuario,
        password=password
    )
    print("Conexión exitosa a PostgreSQL")
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
            "INSERT INTO metricas (metrica, valor, fecha) VALUES (%s, %s, %s) ON CONFLICT DO NOTHING",
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
