from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count, min as _min, max as _max, avg as _avg
import pytest

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .getOrCreate()


# 1. Test de total de km recorridos por provincia:
def test_total_kilometros_por_provincia(spark_session):
    # Dataframe intermedio ya unido
    df_merged = spark_session.createDataFrame(
        [
            (118090887, 'San José', 15.5),
            (118090887, 'San José', 20.0),
            (123456789, 'Heredia', 10.0),
            (123456789, 'Heredia', 12.0),
        ],
        ['Cedula', 'Provincia', 'Kilometros']
    )

    # calculo de km total por provincia
    df_total_km_provincia = df_merged.groupBy("Provincia") \
                                     .agg(_sum("Kilometros").alias("Kilometros_Totales"))

    # Datos esperados
    expected_ds = spark_session.createDataFrame(
        [
            ('San José', 35.5),
            ('Heredia', 22.0)
        ],
        ['Provincia', 'Kilometros_Totales']
    )

    actual_rows = [row.asDict() for row in df_total_km_provincia.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Provincia']) == sorted(expected_rows, key=lambda x: x['Provincia'])


# 2. Test de km recorridos por ciclista por día (incluyendo suma de actividades el mismo día):
def test_kilometros_por_ciclista_por_dia(spark_session):
    # Dataframe intermedio ya unido
    df_merged = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', '2024-10-01', 15.5),
            (118090887, 'Juan Perez', '2024-10-01', 10.0),  # juancito tiene 2 actividades el mismo dia
            (123456789, 'Maria Gomez', '2024-10-01', 10.0),
            (123456789, 'Maria Gomez', '2024-10-03', 12.0),
        ],
        ['Cedula', 'Nombre', 'Fecha', 'Kilometros']
    )

    # se suma actividades del mismo día
    df_km_por_dia = df_merged.groupBy("Cedula", "Nombre", "Fecha") \
                             .agg(_sum("Kilometros").alias("Kilometros_Diarios"))

    # Datos esperados
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', '2024-10-01', 25.5),  # Suma de 15.5 + 10.0 el mismo día
            (123456789, 'Maria Gomez', '2024-10-01', 10.0),
            (123456789, 'Maria Gomez', '2024-10-03', 12.0),
        ],
        ['Cedula', 'Nombre', 'Fecha', 'Kilometros_Diarios']
    )

    actual_rows = [row.asDict() for row in df_km_por_dia.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Fecha']) == sorted(expected_rows, key=lambda x: x['Fecha'])


# 3. Test de total de actividades por ciclista:
def test_total_actividades_por_ciclista(spark_session):
    # Dataframe intermedio ya unido
    df_merged = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', '2024-10-01', 15.5),
            (118090887, 'Juan Perez', '2024-10-01', 20.0),  # Juan tiene 2 actividades el mismo día
            (123456789, 'Maria Gomez', '2024-10-01', 10.0),
            (123456789, 'Maria Gomez', '2024-10-03', 12.0),
        ],
        ['Cedula', 'Nombre', 'Fecha', 'Kilometros']
    )

    # Cálculo del total de actividades por ciclista (contando todas las actividades)
    df_total_actividades = df_merged.groupBy("Cedula", "Nombre") \
                                    .agg(count("*").alias("Total_Actividades"))  # Contar todas las filas

    # Datos esperados
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 2),  # 2 actividades para juan en la misma fecha pero diferentes actividades
            (123456789, 'Maria Gomez', 2),  # 2 ctividades, en fechas diferentes para maría
        ],
        ['Cedula', 'Nombre', 'Total_Actividades']
    )

    actual_rows = [row.asDict() for row in df_total_actividades.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])

    
# 4. Test de ciclistas con actividades en días diferentes en la misma ruta: se maneja como actividades separadas
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import count
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .getOrCreate()

# 4. Test de ciclistas con actividades en días diferentes en la misma ruta: se maneja como actividades separadas
def test_ciclistas_actividades_diferentes_dias_misma_ruta(spark_session):
    
    df_intermedio = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 'Ventolera Escazú', 10.0, '2024-10-01'),
            (118090887, 'Juan Perez', 'San José', 'Ventolera Escazú', 10.0, '2024-10-02'),
            (123456789, 'Maria Gomez', 'Heredia', 'Ventolera Escazú', 10.0, '2024-10-01')
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Agrupar por ciclista y ruta, contando las actividades en dias diferentes
    actual_ds = df_intermedio.groupBy('Cedula', 'Nombre', 'Provincia', 'Nombre_Ruta')\
                             .agg(count('Fecha').alias('Cantidad_Actividades'))

    # Dataframe esperado
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 'Ventolera Escazú', 2),
            (123456789, 'Maria Gomez', 'Heredia', 'Ventolera Escazú', 1)
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Nombre_Ruta', 'Cantidad_Actividades']
    )

    actual_rows = [row.asDict() for row in actual_ds.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])


# 5. Test de la distancia mínima recorrida por ciclista en el mismo día y en días diferentes:
def test_minima_distancia_por_dia(spark_session):
    # Dataframe intermedio ya unido
    df_merged = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', '2024-10-01', 15.5),
            (118090887, 'Juan Perez', '2024-10-01', 20.0),  # Juan tiene 2 actividades el mismo dia
            (123456789, 'Maria Gomez', '2024-10-02', 10.0),  # maria tiene actividad en un dia
            (123456789, 'Maria Gomez', '2024-10-03', 12.0),  # y en otro dia
        ],
        ['Cedula', 'Nombre', 'Fecha', 'Kilometros']
    )

    # se calcula la distancia min
    df_min_km_dia = df_merged.groupBy("Cedula", "Nombre", "Fecha") \
                             .agg(_min("Kilometros").alias("Minima_Distancia"))

    # datos esperados
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', '2024-10-01', 15.5),  # el min de juan es 15.5 el mismo día
            (123456789, 'Maria Gomez', '2024-10-02', 10.0),  # el min de maria en un día es 10.0
            (123456789, 'Maria Gomez', '2024-10-03', 12.0),  # el min de maria en el otro dia es 12.0
        ],
        ['Cedula', 'Nombre', 'Fecha', 'Minima_Distancia']
    )

    actual_rows = [row.asDict() for row in df_min_km_dia.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: (x['Cedula'], x['Fecha'])) == sorted(expected_rows, key=lambda x: (x['Cedula'], x['Fecha']))


# 6. Test de la distancia máxima recorrida por ciclista en el mismo día y en días diferentes:
def test_maxima_distancia_por_dia(spark_session):
    # Dataframe intermedio ya unido
    df_merged = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', '2024-10-01', 15.5),
            (118090887, 'Juan Perez', '2024-10-01', 20.0),  # juan tiene 2 actividades el mismo dia
            (123456789, 'Maria Gomez', '2024-10-02', 10.0),  # maria tiene ctividad en un día
            (123456789, 'Maria Gomez', '2024-10-03', 12.0),  # y actividad en oto dia
        ],
        ['Cedula', 'Nombre', 'Fecha', 'Kilometros']
    )

    # calculo de la distancia max recorrida
    df_max_km_dia = df_merged.groupBy("Cedula", "Nombre", "Fecha") \
                             .agg(_max("Kilometros").alias("Maxima_Distancia"))

    # datos esperados
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', '2024-10-01', 20.0),  # max juan es 20.0 el mismo día
            (123456789, 'Maria Gomez', '2024-10-02', 10.0),  # max de maría en un día es 10.0
            (123456789, 'Maria Gomez', '2024-10-03', 12.0),  # max maría en el otro día es 12.0
        ],
        ['Cedula', 'Nombre', 'Fecha', 'Maxima_Distancia']
    )

    actual_rows = [row.asDict() for row in df_max_km_dia.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: (x['Cedula'], x['Fecha'])) == sorted(expected_rows, key=lambda x: (x['Cedula'], x['Fecha']))
    
# 7. Test de promedio de km  por provincia
def test_promedio_kilometros_por_provincia(spark_session):
      # Dataframe intermedio ya unido
    df_merged = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'Heredia', 50.0),
            (123456789, 'Maria Gomez', 'Heredia', 70.0),
            (987654321, 'Luis Solano', 'San José', 40.0),
            (111222333, 'Ana Rojas', 'San José', 60.0),
            (135790246, 'Carlos Mora', 'Heredia', 30.0),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Kilometros']
    )

    df_promedio_km_provincia = df_merged.groupBy("Provincia") \
                                        .agg(_avg("Kilometros").alias("Promedio_Kilometros"))

    expected_ds = spark_session.createDataFrame(
        [
            ('Heredia', 50.0),
            ('San José', 50.0),
        ],
        ['Provincia', 'Promedio_Kilometros']
    )

    actual_rows = [row.asDict() for row in df_promedio_km_provincia.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Provincia']) == sorted(expected_rows, key=lambda x: x['Provincia'])


# 8. Test de total de km por ciclista y dia
def test_total_kilometros_por_dia(spark_session):
      # Dataframe intermedio ya unido
    df_merged = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', '2024-10-01', 15.0),
            (118090887, 'Juan Perez', '2024-10-01', 10.0),
            (123456789, 'Maria Gomez', '2024-10-02', 20.0),
            (123456789, 'Maria Gomez', '2024-10-03', 12.0),
        ],
        ['Cedula', 'Nombre', 'Fecha', 'Kilometros']
    )

    df_total_km_por_dia = df_merged.groupBy("Cedula", "Nombre", "Fecha") \
                                   .agg(_sum("Kilometros").alias("Kilometros_Totales"))

    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', '2024-10-01', 25.0),
            (123456789, 'Maria Gomez', '2024-10-02', 20.0),
            (123456789, 'Maria Gomez', '2024-10-03', 12.0),
        ],
        ['Cedula', 'Nombre', 'Fecha', 'Kilometros_Totales']
    )

    actual_rows = [row.asDict() for row in df_total_km_por_dia.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: (x['Cedula'], x['Fecha'])) == sorted(expected_rows, key=lambda x: (x['Cedula'], x['Fecha']))


# 9. Test de suma de km por ciclista y provincia
def test_suma_kilometros_por_provincia(spark_session):
      # Dataframe intermedio ya unido
    df_merged = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'Heredia', 50.0),
            (123456789, 'Maria Gomez', 'Heredia', 70.0),
            (987654321, 'Luis Solano', 'San José', 40.0),
            (111222333, 'Ana Rojas', 'San José', 60.0),
            (135790246, 'Carlos Mora', 'Heredia', 30.0),
            (246813579, 'Laura Castro', 'Heredia', 90.0),
            (999999999, 'Diego Gómez', 'San José', 85.0),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Kilometros']
    )

    df_suma_km = df_merged.groupBy("Cedula", "Nombre", "Provincia") \
                          .agg(_sum("Kilometros").alias("Kilometros_Totales"))

    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'Heredia', 50.0),
            (123456789, 'Maria Gomez', 'Heredia', 70.0),
            (987654321, 'Luis Solano', 'San José', 40.0),
            (111222333, 'Ana Rojas', 'San José', 60.0),
            (135790246, 'Carlos Mora', 'Heredia', 30.0),
            (246813579, 'Laura Castro', 'Heredia', 90.0),
            (999999999, 'Diego Gómez', 'San José', 85.0),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Kilometros_Totales']
    )

    actual_rows = [row.asDict() for row in df_suma_km.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])


# 10. Test de total de km recorridos por dia por provincia
def test_total_kilometros_por_dia_por_provincia(spark_session):
      # Dataframe intermedio ya unido
    df_merged = spark_session.createDataFrame(
        [
            (118090887, 'San José', '2024-10-01', 20.0),
            (123456789, 'Heredia', '2024-10-02', 30.0),
            (987654321, 'San José', '2024-10-01', 40.0),
            (111222333, 'Heredia', '2024-10-02', 15.0),
        ],
        ['Cedula', 'Provincia', 'Fecha', 'Kilometros']
    )

    df_total_km_provincia = df_merged.groupBy("Provincia", "Fecha") \
                                     .agg(_sum("Kilometros").alias("Kilometros_Totales"))

    expected_ds = spark_session.createDataFrame(
        [
            ('San José', '2024-10-01', 60.0),
            ('Heredia', '2024-10-02', 45.0),
        ],
        ['Provincia', 'Fecha', 'Kilometros_Totales']
    )

    actual_rows = [row.asDict() for row in df_total_km_provincia.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: (x['Provincia'], x['Fecha'])) == sorted(expected_rows, key=lambda x: (x['Provincia'], x['Fecha']))


if __name__ == "__main__":
    spark = spark_session()
    test_total_kilometros_por_provincia(spark)
    test_kilometros_por_ciclista_por_dia(spark)
    test_total_actividades_por_ciclista(spark)
    test_ciclistas_actividades_diferentes_dias_misma_ruta(spark)
    test_minima_distancia_por_dia(spark)
    test_maxima_distancia_por_dia(spark)
    test_promedio_kilometros_por_provincia(spark)
    test_total_kilometros_por_dia(spark)
    test_suma_kilometros_por_provincia(spark)
    test_total_kilometros_por_dia_por_provincia(spark)
    
    print("los tests pasaron bien")

