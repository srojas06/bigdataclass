from pyspark.sql import SparkSession

def iniciar_spark(app_name="DataProcessing"):
    """
    Inicia una sesión de Spark.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_data(spark, census_path, crimes_path):
    """
    Carga los datos de los archivos CSV en DataFrames de Spark.
    """
    census_df = spark.read.csv(census_path, header=True, inferSchema=True)
    crimes_df = spark.read.csv(crimes_path, header=True, inferSchema=True)
    return census_df, crimes_df

def preprocess_census_data(df):
    """
    Preprocesa el DataFrame del censo eliminando valores nulos y aplicando
    transformaciones necesarias.
    """
    df = df.na.drop()  # Elimina filas con valores nulos
    return df

def preprocess_crimes_data(df):
    """
    Preprocesa el DataFrame de crímenes eliminando la columna 'Year' y valores nulos.
    """
    df = df.drop("year")  # Elimina la columna 'Year' para evitar patrones temporales
    df = df.na.drop()  # Elimina filas con valores nulos
    return df

def save_to_postgres(df, table_name, jdbc_url, properties):
    """
    Escribe un DataFrame en PostgreSQL.
    """
    df.write.jdbc(url=jdbc_url, table=table_name, mode="overwrite", properties=properties)
