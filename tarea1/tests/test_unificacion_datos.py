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


#2 Este test se enfoca en verificar que el sistema maneje correctamente a ciclistas con el mismo nombre pero diferente cédula.
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


#3 Este test verifica que si un ciclista realiza la misma actividad más de una vez en la misma ruta y en el mismo dia, se cuenten correctamente las repeticiones.
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

    # Agrupar por ciclista y ruta, contando las actividades repetidas
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



#4 Este test asegura que un ciclista sin actividades aún aparezca en el resultado, y que las rutas sin actividades no causen problemas.
def test_rutas_sin_actividades(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10.0)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    # Crear un DataFrame vacío con un esquema explícito para las actividades
    schema_actividades = StructType([
        StructField("Codigo_Ruta", IntegerType(), True),
        StructField("Cedula", IntegerType(), True),
        StructField("Fecha", StringType(), True)
    ])
    
    # El DataFrame vacío con el esquema explícito
    df_actividades = spark_session.createDataFrame([], schema_actividades)

    # Unión de ciclistas con actividades (left join para mantener los ciclistas aunque no tengan actividades)
    actual_ds = df_ciclistas.join(df_actividades, on="Cedula", how="left")

    # Unión con las rutas (left join para mantener las actividades sin rutas)
    actual_ds = actual_ds.join(df_rutas, on="Codigo_Ruta", how="left")

    # Datos esperados (ciclista sin actividad ni ruta)
    expected_data = [
        (118090887, 'Juan Perez', 'San José', None, None, None, None)
    ]
    
  
    expected_schema = StructType([
        StructField("Cedula", IntegerType(), True),
        StructField("Nombre", StringType(), True),
        StructField("Provincia", StringType(), True),
        StructField("Codigo_Ruta", IntegerType(), True),
        StructField("Nombre_Ruta", StringType(), True),
        StructField("Kilometros", DoubleType(), True),
        StructField("Fecha", StringType(), True),
    ])
    
    expected_ds = spark_session.createDataFrame(expected_data, expected_schema)

 
    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    print("Actual Rows:", sorted(actual_rows, key=lambda x: x['Cedula']))
    print("Expected Rows:", sorted(expected_rows, key=lambda x: x['Cedula']))

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])

#5 Este test verifica que las actividades en rutas distintas el mismo día se mantengan separadas en el resultado.
def test_ciclistas_actividades_diferentes_rutas_mismo_dia(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10.0), (2, 'Ruta Cartago', 15.0)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    # Datos de actividades, dos rutas diferentes el mismo día
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (2, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Realizar la unión y el procesamiento de los datos
    actual_ds = df_ciclistas.join(df_actividades, on="Cedula", how="outer")
    actual_ds = actual_ds.join(df_rutas, on="Codigo_Ruta", how="left")

    # Datos esperados
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10.0, '2024-10-01'),
            (118090887, 'Juan Perez', 'San José', 2, 'Ruta Cartago', 15.0, '2024-10-01')
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Comparación de resultados
    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    # Print de resultados para depuración
    print("Actual Rows:", sorted(actual_rows, key=lambda x: x['Cedula']))
    print("Expected Rows:", sorted(expected_rows, key=lambda x: x['Cedula']))

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])


#6 Este test verifica que las actividades en días diferentes en la misma ruta se manejen como actividades separadas. 
def test_ciclistas_actividades_diferentes_dias_misma_ruta(spark_session):
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

    # Datos de actividades, diferentes días en la misma ruta
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (1, 118090887, '2024-10-02')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Realizar la unión y el procesamiento de los datos
    actual_ds = df_ciclistas.join(df_actividades, on="Cedula", how="outer")
    actual_ds = actual_ds.join(df_rutas, on="Codigo_Ruta", how="left")

    # Datos esperados
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10.0, '2024-10-01'),
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10.0, '2024-10-02')
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Comparación de resultados
    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    
    print("Actual Rows:", sorted(actual_rows, key=lambda x: x['Cedula']))
    print("Expected Rows:", sorted(expected_rows, key=lambda x: x['Cedula']))

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])

#7 Este test verifica que  as rutas sin actividades asignadas se manejen correctamente, con valores nulos para las actividades.
def test_rutas_sin_actividades(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10.0), (2, 'Ruta Cartago', 15.0)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    # Crear un DataFrame vacío con un esquema explícito para las actividades
    schema_actividades = StructType([
        StructField("Codigo_Ruta", IntegerType(), True),
        StructField("Cedula", IntegerType(), True),
        StructField("Fecha", StringType(), True)
    ])
    
    # DataFrame vacío con esquema
    df_actividades = spark_session.createDataFrame([], schema_actividades)

    # Unión de ciclistas con actividades (left join para mantener los ciclistas aunque no tengan actividades)
    actual_ds = df_ciclistas.join(df_actividades, on="Cedula", how="left")

    # Unión con las rutas (left join para mantener las actividades sin rutas)
    actual_ds = actual_ds.join(df_rutas, on="Codigo_Ruta", how="left")

    # Datos esperados (ciclista sin actividad ni ruta)
    expected_data = [
        (118090887, 'Juan Perez', 'San José', None, None, None, None)
    ]
    
    expected_schema = StructType([
        StructField("Cedula", IntegerType(), True),
        StructField("Nombre", StringType(), True),
        StructField("Provincia", StringType(), True),
        StructField("Codigo_Ruta", IntegerType(), True),
        StructField("Nombre_Ruta", StringType(), True),
        StructField("Kilometros", DoubleType(), True),
        StructField("Fecha", StringType(), True),
    ])
    
    expected_ds = spark_session.createDataFrame(expected_data, expected_schema)

    # Comparación de resultados
    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    # Print de resultados para depuración
    print("Actual Rows:", sorted(actual_rows, key=lambda x: x['Cedula']))
    print("Expected Rows:", sorted(expected_rows, key=lambda x: x['Cedula']))

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])

