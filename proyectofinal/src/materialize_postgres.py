from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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

def preprocess_census_data(df):
    """
    Preprocesa el DataFrame del censo realizando imputaciones en valores nulos.
    """
    # Imputación de valores nulos (ejemplo: se llenan con la media de la columna)
    for column in df.columns:
        if df.select(column).dtypes[0] in ['double', 'float']:
            mean_value = df.select(F.mean(column)).collect()[0][0]
            df = df.fillna({column: mean_value})
    return df

def preprocess_crimes_data(df):
    """
    Preprocesa el DataFrame de crímenes eliminando la columna 'Year' y realizando imputaciones en valores nulos.
    """
    if 'Year' in df.columns:
        df = df.drop("Year")
    
    # Imputación de valores nulos (ejemplo: se llenan con la media de la columna)
    for column in df.columns:
        if df.select(column).dtypes[0] in ['double', 'float']:
            mean_value = df.select(F.mean(column)).collect()[0][0]
            df = df.fillna({column: mean_value})
    return df

def save_to_postgres(df, table_name, url, properties):
    """
    Guarda el DataFrame en una tabla PostgreSQL.
    """
    print(f"Intentando guardar datos en PostgreSQL en la tabla '{table_name}'...")
    print("Esquema del DataFrame:")
    df.printSchema()  # Imprimir esquema del DataFrame
    
    try:
        df.write.jdbc(url=url, table=table_name, mode="overwrite", properties=properties)
        print(f"Datos guardados en la tabla '{table_name}' con éxito.")
    except Exception as e:
        print(f"Error al guardar datos en PostgreSQL: {e}")

def main():
    # Inicializar Spark y cargar datos
    spark = iniciar_spark()
    print("Sesión de Spark iniciada.")

    # Rutas de los archivos CSV
    census_path = "/src/data/acs2017_census_tract_data.csv"
    crimes_path = "/src/data/estimated_crimes_1979_2019.csv"

    # Cargar datos
    census_df, crimes_df = load_data(spark, census_path, crimes_path)
    print("Datos cargados.")

    # Preprocesar los datos
    census_df = preprocess_census_data(census_df)
    crimes_df = preprocess_crimes_data(crimes_df)
    print("Datos preprocesados.")

    # Configuración de conexión PostgreSQL
    url = "jdbc:postgresql://172.17.0.2:5432/bigdata_db"
    properties = {
        "user": "postgres",
        "password": "testPassword",
        "driver": "org.postgresql.Driver"
    }
    
    # Guardar en PostgreSQL
    save_to_postgres(census_df, "census_data", url, properties)
    save_to_postgres(crimes_df, "crimes_data", url, properties)

    # Realizar el cruce de datos usando la columna 'State' y 'state_name'
    crossed_df = census_df.join(crimes_df, census_df["State"] == crimes_df["state_name"], "inner")
    save_to_postgres(crossed_df, "crossed_data", url, properties)

    # Finalizar Spark
    spark.stop()
    print("Materialización completada.")

if __name__ == "__main__":
    main()
