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
# Este test valida la correcta unión de los datos de ciclistas, rutas y actividades.
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

    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10.0, '2024-10-01'),
            (123456789, 'Maria Gomez', 'Heredia', None, None, None, None),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Comparación más precisa entre los resultados esperados y actuales
    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])




# 2. Test para verificar que los ciclistas sin actividad se manejen correctamente
# Verifica que los ciclistas sin actividad aparezcan en el dataset final con columnas de actividad nulas.
def test_ciclistas_sin_actividad(spark_session):
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

    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10.0, '2024-10-01'),
            (123456789, 'Maria Gomez', 'Heredia', None, None, None, None),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Comparación precisa entre los resultados esperados y actuales
    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])





# 3. Test para ciclistas con el mismo nombre pero diferentes cédulas
# Se prueba que ciclistas con el mismo nombre pero cédulas diferentes no se fusionen incorrectamente.
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

    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula']).withColumnRenamed("Cedula", "Cedula_Actividades")
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta']).withColumnRenamed("Cedula_Actividades", "Cedula")

    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10.0, '2024-10-01'),
            (118090888, 'Juan Perez', 'San José', None, None, None, None),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Comparación precisa
    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])





# 4. Test para ciclistas con actividades repetidas
# Verifica que si un ciclista realiza actividades repetidas en un mismo día, se manejen correctamente.
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

    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula']).withColumnRenamed("Cedula", "Cedula_Actividades")
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta']).withColumnRenamed("Cedula_Actividades", "Cedula")

    # Agrupar por ciclista y ruta, sumando las actividades repetidas
    actual_ds = actual_ds.groupBy("Cedula", "Codigo_Ruta", "Nombre_Ruta", "Kilometros", "Fecha").count()

    expected_ds = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10.0, '2024-10-01', 2)],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha', 'count']
    )

    # Comparación precisa
    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])





# 5. Test para rutas sin actividades
# Verifica que las rutas sin actividades no aparezcan en el dataset final.
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

    # Realizar la unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados
    expected_ds = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José', None, None, None, None)],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Asegurarse de comparar correctamente las filas con 'None'
    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])


