import sys
import funciones2  # Importar las funciones desde funciones2.py
import psycopg2
from pyspark.sql import SparkSession

# Crear la sesión de Spark
spark = SparkSession.builder.appName("PuntosExtrasBigData").getOrCreate()

# Deshabilitar los logs innecesarios de Spark
spark.sparkContext.setLogLevel("ERROR")

# Verificar si se han proporcionado al menos 5 argumentos (host, usuario, password, nombre_bd, puerto opcional)
if len(sys.argv) < 5 or len(sys.argv) > 6:
    print("Uso: programamain.py <host> <usuario> <password> <nombre_bd> [<puerto>]")
    sys.exit(1)

# Definir las rutas de los archivos YAML con 'confecha'
rutas_archivos_yaml = [
    '/src/data2/caja1_confecha.yaml',
    '/src/data2/caja2_confecha.yaml',
    '/src/data2/caja3_confecha.yaml',
    '/src/data2/caja4_confecha.yaml',
    '/src/data2/caja5_confecha.yaml'
]

# Extraer los parámetros de conexión de la base de datos
host = sys.argv[1]  # Argumento para el host
usuario = sys.argv[2]  # Usuario de PostgreSQL
password = sys.argv[3]  # Contraseña
nombre_bd = sys.argv[4]  # Nombre de la base de datos
puerto = "5432"  # Valor por defecto para el puerto

# Verifica si se especificó el puerto como argumento adicional
if len(sys.argv) == 6:
    try:
        puerto = int(sys.argv[5])
    except ValueError:
        print("Error: El puerto debe ser un número entero válido.")
        sys.exit(1)

# Leer y combinar los archivos YAML
try:
    datos_yaml = funciones2.leer_y_combinar_archivos_yaml(rutas_archivos_yaml, spark)
except ValueError as e:
    print(f"Error: {e}")
    sys.exit(1)

# Mostrar el DataFrame creado desde YAML combinado
try:
    print("\n--- DataFrame creado desde YAML combinado ---")
    datos_yaml.show(truncate=False)
except Exception as e:
    print(f"Error mostrando el DataFrame: {e}")
    sys.exit(1)

# Calcular las métricas necesarias
try:
    print("\n--- Iniciando el cálculo de métricas ---")
    caja_con_mas_ventas, caja_con_menos_ventas, percentil_25, percentil_50, percentil_75 = funciones2.calcular_metricas(datos_yaml)
    producto_mas_vendido, producto_mayor_ingreso = funciones2.calcular_productos(datos_yaml)
except Exception as e:
    print(f"Error calculando las métricas: {e}")
    sys.exit(1)

metricas_data = [
    ("caja_con_mas_ventas", caja_con_mas_ventas, "2024/10/16"),
    ("caja_con_menos_ventas", caja_con_menos_ventas, "2024/10/16"),
    ("percentil_25_por_caja", percentil_25, "2024/10/16"),
    ("percentil_50_por_caja", percentil_50, "2024/10/16"),
    ("percentil_75_por_caja", percentil_75, "2024/10/16"),
    ("producto_mas_vendido_por_unidad", producto_mas_vendido, "2024/10/16"),
    ("producto_de_mayor_ingreso", producto_mayor_ingreso, "2024/10/16")
]

# Mostrar las métricas calculadas
print("\n--- Métricas calculadas ---")
for row in metricas_data:
    print(f"Métrica: {row[0]}, Valor: {row[1]}, Fecha: {row[2]}")

# Generar el DataFrame de métricas con fecha
try:
    print("\n--- Generando DataFrame de métricas con fecha ---")
    df_metricas = funciones2.generar_dataframe_metricas_con_fecha(metricas_data, spark)
    df_metricas.show(truncate=False)
except Exception as e:
    print(f"Error generando el DataFrame de métricas con fecha: {e}")
    sys.exit(1)

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
            fecha DATE,
            UNIQUE (metrica, fecha)
        )
    ''')

    # Insertar los datos de las métricas en la tabla, evitando duplicados
    for row in metricas_data:
        cursor.execute('''
            INSERT INTO metricas (metrica, valor, fecha) 
            VALUES (%s, %s, %s) 
            ON CONFLICT (metrica, fecha) DO UPDATE SET valor = EXCLUDED.valor
        ''', (row[0], str(row[1]), row[2]))

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

