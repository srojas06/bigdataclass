import pytest
from pyspark.sql import SparkSession
from funciones import load_data, preprocess_census_data, preprocess_crimes_data

# Configuración de sesión de Spark
@pytest.fixture(scope="module")
def spark_session():
    spark = SparkSession.builder.appName("PruebasProcesamiento").getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    yield spark
    spark.stop()

# 1. Prueba para cargar datos sin errores
def test_load_data(spark_session):
    # Ruta ficticia para verificar si carga correctamente
    census_path = "/src/data/acs2017_census_tract_data.csv"
    crimes_path = "/src/data/estimated_crimes_1979_2019.csv"
    
    census_df, crimes_df = load_data(spark_session, census_path, crimes_path)

    # Verificar que ambos DataFrames no estén vacíos
    assert census_df is not None
    assert crimes_df is not None

# 2. Prueba de preprocesamiento para el censo
def test_preprocess_census_data(spark_session):
    # Crear datos de prueba con un valor nulo
    data = [("California", 50000), (None, 30000)]
    df = spark_session.createDataFrame(data, ["State", "Population"])

    # Aplicar preprocesamiento
    clean_df = preprocess_census_data(df)

    # Confirmar que se eliminen filas con valores nulos
    assert clean_df.count() == 1

# 3. Prueba de preprocesamiento para el conjunto de datos de crímenes
def test_preprocess_crimes_data(spark_session):
    # Datos de prueba con la columna "Year" que debe eliminarse
    data = [("California", 1000, 2020), ("Texas", 1500, 2019)]
    df = spark_session.createDataFrame(data, ["state_name", "Crimes", "Year"])

    # Aplicar preprocesamiento
    clean_df = preprocess_crimes_data(df)

    # Confirmar que la columna "Year" se ha eliminado
    assert "Year" not in clean_df.columns
    # Verificar que no haya valores nulos en el DataFrame procesado
    assert clean_df.filter(clean_df["state_name"].isNull()).count() == 0
