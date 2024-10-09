from joiner import join_dataframes
import pytest
from pyspark.sql.functions import count
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .getOrCreate()


# 1. Test para verificar que la unión de los datos sea correcta
def test_union_correcta(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (123456789, 'Maria Gomez', 'Heredia')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10.0)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Usamos una unión externa (outer join) para asegurar que todos los ciclistas estén presentes.
    actual_ds = df_ciclistas.join(df_actividades, on="Cedula", how="outer")
    actual_ds = actual_ds.join(df_rutas, on="Codigo_Ruta", how="left")

    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10.0, '2024-10-01'),
            (123456789, 'Maria Gomez', 'Heredia', None, None, None, None),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    print("Actual Rows:", sorted(actual_rows, key=lambda x: x['Cedula']))
    print("Expected Rows:", sorted(expected_rows, key=lambda x: x['Cedula']))

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])


#2
def test_ciclistas_mismo_nombre_diferente_cedula(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (118090888, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10.0)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    actual_ds = df_ciclistas.join(df_actividades, on="Cedula", how="outer")
    actual_ds = actual_ds.join(df_rutas, on="Codigo_Ruta", how="left")

    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10.0, '2024-10-01'),
            (118090888, 'Juan Perez', 'San José', None, None, None, None),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    print("Actual Rows:", sorted(actual_rows, key=lambda x: x['Cedula']))
    print("Expected Rows:", sorted(expected_rows, key=lambda x: x['Cedula']))

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])


#3
def test_ciclistas_actividades_repetidas(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10.0)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    actual_ds = df_ciclistas.join(df_actividades, on="Cedula", how="outer")
    actual_ds = actual_ds.join(df_rutas, on="Codigo_Ruta", how="left")

    # Eliminar duplicados antes de agrupar
    actual_ds = actual_ds.dropDuplicates(['Cedula', 'Codigo_Ruta', 'Fecha'])

    # Agrupar por ciclista y ruta
    actual_ds = actual_ds.groupBy("Cedula", "Codigo_Ruta", "Nombre_Ruta", "Kilometros", "Fecha").count()

    expected_ds = spark_session.createDataFrame(
        [(118090887, 1, 'Ventolera Escazú', 10.0, '2024-10-01', 2)],
        ['Cedula', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha', 'count']
    )

    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    print("Actual Rows:", sorted(actual_rows, key=lambda x: x['Cedula']))
    print("Expected Rows:", sorted(expected_rows, key=lambda x: x['Cedula']))

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])


#4
def test_rutas_sin_actividades(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10.0)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    # Crear un DataFrame vacío con un esquema explícito
    schema = StructType([
        StructField("Codigo_Ruta", IntegerType(), True),
        StructField("Cedula", IntegerType(), True),
        StructField("Fecha", StringType(), True)
    ])
    df_actividades = spark_session.createDataFrame([], schema)

    # Unión de ciclistas con actividades
    actual_ds = df_ciclistas.join(df_actividades, on="Cedula", how="left")
    actual_ds = actual_ds.join(df_rutas, on="Codigo_Ruta", how="left")

    # Datos esperados
    expected_ds = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José', None, None, None, None)],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    print("Actual Rows:", sorted(actual_rows, key=lambda x: x['Cedula']))
    print("Expected Rows:", sorted(expected_rows, key=lambda x: x['Cedula']))

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])


