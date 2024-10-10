from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, count
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


# 2. Test de km recorridos por ciclista por dia:
def test_kilometros_por_ciclista_por_dia(spark_session):
    # Dataframe intermedio ya unido
    df_merged = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', '2024-10-01', 15.5),
            (118090887, 'Juan Perez', '2024-10-02', 20.0),
            (123456789, 'Maria Gomez', '2024-10-01', 10.0),
            (123456789, 'Maria Gomez', '2024-10-03', 12.0),
        ],
        ['Cedula', 'Nombre', 'Fecha', 'Kilometros']
    )

    # Cálculo de kilómetros recorridos por día
    df_km_por_dia = df_merged.groupBy("Cedula", "Nombre", "Fecha") \
                             .agg(_sum("Kilometros").alias("Kilometros_Diarios"))

    # Datos esperados
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', '2024-10-01', 15.5),
            (118090887, 'Juan Perez', '2024-10-02', 20.0),
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
            (118090887, 'Juan Perez', '2024-10-02', 20.0),
            (123456789, 'Maria Gomez', '2024-10-01', 10.0),
            (123456789, 'Maria Gomez', '2024-10-03', 12.0),
        ],
        ['Cedula', 'Nombre', 'Fecha', 'Kilometros']
    )

    # Cálculo del total de actividades por ciclista
    df_total_actividades = df_merged.groupBy("Cedula", "Nombre") \
                                    .agg(count("Fecha").alias("Total_Actividades"))

    # Datos esperados
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 2),
            (123456789, 'Maria Gomez', 2)
        ],
        ['Cedula', 'Nombre', 'Total_Actividades']
    )

    actual_rows = [row.asDict() for row in df_total_actividades.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])


if __name__ == "__main__":
    spark = spark_session()
    test_total_kilometros_por_provincia(spark)
    test_kilometros_por_ciclista_por_dia(spark)
    test_total_actividades_por_ciclista(spark)
    print("Todos los tests pasaron correctamente.")
