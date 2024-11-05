from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def iniciar_spark(app_name="DataProcessing"):
    """
    Inicia una sesión de Spark.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()

def load_data(spark, census_path, crimes_path):
    """
    Carga los datos de los archivos CSV en DataFrames de Spark.
    """
    # Leer el archivo de censo
    census_df = spark.read.csv(census_path, header=True, inferSchema=True)
    # Leer el archivo de crímenes
    crimes_df = spark.read.csv(crimes_path, header=True, inferSchema=True)
    return census_df, crimes_df

def preprocess_census_data(df):
    """
    Preprocesa el DataFrame del censo eliminando filas con valores nulos.
    """
    df = df.na.drop()  # Elimina filas con valores nulos
    return df

def preprocess_crimes_data(df):
    """
    Preprocesa el DataFrame de crímenes eliminando la columna 'Year' y filas con valores nulos.
    """
    # Eliminar la columna 'Year' si existe
    if 'Year' in df.columns:
        df = df.drop("Year")
    # Eliminar filas con valores nulos
    df = df.na.drop()
    return df
