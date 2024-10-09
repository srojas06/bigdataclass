from joiner import join_dataframes
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType


@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .getOrCreate()


# 1. Test para la unión correcta de los datos
# Este test valida la correcta unión de los datos de ciclistas, rutas y actividades.
def test_union_correcta(spark_session):
    # Datos de los ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (123456789, 'Maria Gomez', 'Heredia')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    # Datos de las rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    # Datos de las actividades
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])

    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
            (123456789, 'Maria Gomez', 'Heredia', None, None, None, None),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de los resultados
    assert actual_ds.collect() == expected_ds.collect()


# 2. Test para ciclistas sin actividad
# Verifica que los ciclistas sin actividad aparezcan en el dataset final con columnas de actividad nulas.
def test_ciclistas_sin_actividad(spark_session):
    # Datos de los ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (123456789, 'Maria Gomez', 'Heredia')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    # Datos de las rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    # Datos de actividades (solo una actividad)
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])

    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados: Maria Gomez no tiene actividad, su columna de actividad es nula
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
            (123456789, 'Maria Gomez', 'Heredia', None, None, None, None),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de los resultados
    assert actual_ds.collect() == expected_ds.collect()


# 3. Test para ciclistas con el mismo nombre pero diferentes cédulas
# Se prueba que ciclistas con el mismo nombre pero cédulas diferentes no se fusionen incorrectamente.
def test_ciclistas_mismo_nombre_diferente_cedula(spark_session):
    # Datos de ciclistas con mismo nombre pero diferente cédula
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (118090888, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    # Datos de las rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    # Datos de las actividades (solo uno de los Juan Perez tiene actividad)
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])

    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados: Ambos Juan Perez deben aparecer por separado
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
            (118090888, 'Juan Perez', 'San José', None, None, None, None),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de resultados
    assert actual_ds.collect() == expected_ds.collect()


# 4. Test para ciclistas con actividades repetidas
# Verifica que si un ciclista realiza actividades repetidas en un mismo día, se manejen correctamente.
def test_ciclistas_actividades_repetidas(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10), (2, 'Ruta Las Cruces', 5.5)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    # Datos de actividades repetidas en un mismo día
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (2, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])

    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados: Ambas actividades deben aparecer por separado
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
            (118090887, 'Juan Perez', 'San José', 2, 'Ruta Las Cruces', 5.5, '2024-10-01'),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de resultados
    assert actual_ds.collect() == expected_ds.collect()


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
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    # Sin actividades
    df_actividades = spark_session.createDataFrame([], ['Codigo_Ruta', 'Cedula', 'Fecha'])

    # Unión de ciclistas con actividades (no habrá resultados)
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])

    # Verificación de resultados
    assert actual_ds.count() == 0


# 6. Test para ciclistas sin actividad total
# Verifica que los ciclistas sin ninguna actividad aparezcan con datos nulos.
def test_ciclistas_sin_actividad_total(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (123456789, 'Maria Lopez', 'Heredia')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    # Sin actividades
    df_actividades = spark_session.createDataFrame([], ['Codigo_Ruta', 'Cedula', 'Fecha'])

    # Unión de ciclistas con actividades (Juan Perez tiene actividad, Maria Lopez no)
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])

    # Verificación de resultados
    assert actual_ds.filter(actual_ds['Nombre'] == 'Maria Lopez').count() == 1

# 7. Test para múltiples actividades en un día
# Verifica que si un ciclista realiza múltiples actividades en el mismo día, estas se registren correctamente.
def test_multiples_actividades_un_dia(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10), (2, 'Ruta Las Cruces', 5.5)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (2, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
            (118090887, 'Juan Perez', 'San José', 2, 'Ruta Las Cruces', 5.5, '2024-10-01'),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    assert actual_ds.collect() == expected_ds.collect()

# 8. Test para actividades duplicadas
# Verifica que si se duplican las actividades, solo una sea registrada.
def test_actividades_duplicadas(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    # Actividades duplicadas para el mismo ciclista
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    expected_ds = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01')],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    assert actual_ds.collect() == expected_ds.collect()

# 9. Test para datos faltantes en ciclistas
# Verifica que los ciclistas con datos faltantes aparezcan correctamente.
def test_datos_faltantes_ciclistas(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, None, 'San José'), (123456789, 'Maria Lopez', None)],
        ['Cedula', 'Nombre', 'Provincia']
    )

    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    expected_ds = spark_session.createDataFrame(
        [(118090887, None, 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
         (123456789, 'Maria Lopez', None, None, None, None, None)],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    assert actual_ds.collect() == expected_ds.collect()

# 10. Test para ciclistas con múltiples actividades en días diferentes
# Verifica que si un ciclista realiza múltiples actividades en diferentes días, se registren correctamente.
def test_ciclistas_multiples_actividades_dias(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )

    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )

    # Actividades en días diferentes
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (1, 118090887, '2024-10-02')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-02'),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    assert actual_ds.collect() == expected_ds.collect()
