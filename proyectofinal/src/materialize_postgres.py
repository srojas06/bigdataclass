from pyspark.sql import SparkSession
import psycopg2
import sys

def iniciar_spark():
    """
    Inicia la sesión de Spark.
    """
    return SparkSession.builder \
        .appName("MaterializePostgres") \
        .config("spark.jars", "/src/postgresql-42.2.14.jar") \
        .getOrCreate()

def load_data(spark, census_path, crimes_path):
    """
    Carga los datos de los archivos CSV en DataFrames de Spark.
    """
    census_df = spark.read.csv(census_path, header=True, inferSchema=True)
    crimes_df = spark.read.csv(crimes_path, header=True, inferSchema=True)
    return census_df, crimes_df

def save_to_postgres(df, table_name, connection_params):
    """
    Guarda el DataFrame en una tabla PostgreSQL.
    """
    try:
        conn = psycopg2.connect(**connection_params)
        print("Conexión exitosa a PostgreSQL")
        cursor = conn.cursor()
        
        # Crea la tabla si no existe
        cursor.execute(f'''
            CREATE TABLE IF NOT EXISTS {table_name} (
                id SERIAL PRIMARY KEY,
                data JSONB
            )
        ''')

        # Insertar los datos de DataFrame a PostgreSQL
        for row in df.collect():
            cursor.execute(f'''
                INSERT INTO {table_name} (data) VALUES (%s)
            ''', (row.asDict(),))

        conn.commit()
        print(f"Datos guardados en la tabla {table_name}.")

    except Exception as e:
        print(f"Error al guardar datos en PostgreSQL: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Conexión a PostgreSQL cerrada")

def main():
    # Inicializar Spark
    spark = iniciar_spark()
    print("Sesión de Spark iniciada.")

    # Rutas de los archivos CSV
    census_path = "/src/data/acs2017_census_tract_data.csv"
    crimes_path = "/src/data/estimated_crimes_1979_2019.csv"

    # Cargar datos
    census_df, crimes_df = load_data(spark, census_path, crimes_path)
    print("Datos cargados.")

    # Parámetros de conexión a PostgreSQL
    connection_params = {
        "host": "172.17.0.2",  # IP del contenedor de PostgreSQL
        "database": "bigdata_db",
        "user": "postgres",
        "password": "testPassword"
    }

    # Guardar DataFrames en PostgreSQL
    save_to_postgres(census_df, "census_data", connection_params)
    save_to_postgres(crimes_df, "crimes_data", connection_params)

    # Finalizar Spark
    spark.stop()
    print("Materialización completada.")

if __name__ == "__main__":
    main()
