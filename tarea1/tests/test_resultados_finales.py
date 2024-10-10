from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, avg as _avg, count, col, collect_list, expr, struct, explode
import pytest

@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder \
        .appName("TestSession") \
        .master("local[*]") \
        .getOrCreate()


# 1. Test de top N ciclistas por total de kilómetros recorridos:
def test_top_n_ciclistas_por_km(spark_session):
    # DataFrame intermedio con datos de ciclistas
    df_merged = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 50.0),
            (123456789, 'Maria Gomez', 'Heredia', 70.0),
            (987654321, 'Luis Solano', 'San José', 40.0),
            (111222333, 'Ana Rojas', 'San José', 60.0),
            (135790246, 'Carlos Mora', 'Heredia', 30.0),
            (222222222, 'Sofía Rodríguez', 'San José', 20.0),
            (333333333, 'Diego Sánchez', 'Heredia', 80.0),
            (444444444, 'Laura Méndez', 'Heredia', 50.0),
            (555555555, 'Javier Díaz', 'San José', 90.0),  # Javier tiene 90 km
            (666666666, 'Fernanda Ortiz', 'Heredia', 100.0),
            (777777777, 'Pedro González', 'San José', 75.0),
            (888888888, 'Isabella Cruz', 'San José', 85.0),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Kilometros']
    )

    # Agrupación por ciclista y provincia, sumando los kilómetros
    df_top_n = df_merged.groupBy("Cedula", "Nombre", "Provincia") \
                         .agg(_sum("Kilometros").alias("Kilometros_Totales")) \
                         .orderBy("Provincia", "Kilometros_Totales", ascending=False)

    # Se obtiene el top N (en este caso, top 5)
    n = 5
    df_top_n_filtered = df_top_n.groupBy("Provincia") \
                                  .agg(collect_list(struct("Cedula", "Nombre", "Kilometros_Totales")).alias("Top_Ciclistas")) \
                                  .withColumn("Top_Ciclistas", expr(f"slice(Top_Ciclistas, 1, {n})"))

    # Se ordena el resultado para que el ciclista con mayor km esté primero
    df_top_n_sorted = df_top_n_filtered.select(
        "Provincia", 
        explode("Top_Ciclistas").alias("Ciclista")
    ).select(
        "Provincia",
        "Ciclista.Cedula",
        "Ciclista.Nombre",
        "Ciclista.Kilometros_Totales"
    ).orderBy("Provincia", "Ciclista.Kilometros_Totales", ascending=False)

    # Datos esperados
    expected_ds = spark_session.createDataFrame(
        [
            ('San José', 555555555, 'Javier Díaz', 90.0), 
            ('San José', 888888888, 'Isabella Cruz', 85.0),
            ('San José', 777777777, 'Pedro González', 75.0),
            ('San José', 111222333, 'Ana Rojas', 60.0),
            ('San José', 118090887, 'Juan Perez', 50.0),
            ('Heredia', 666666666, 'Fernanda Ortiz', 100.0),
            ('Heredia', 333333333, 'Diego Sánchez', 80.0),
            ('Heredia', 123456789, 'Maria Gomez', 70.0),
            ('Heredia', 444444444, 'Laura Méndez', 50.0),
            ('Heredia', 135790246, 'Carlos Mora', 30.0),
        ],
        ['Provincia', 'Cedula', 'Nombre', 'Kilometros_Totales']
    )

    actual_rows = [row.asDict() for row in df_top_n_sorted.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: (x['Provincia'], x['Kilometros_Totales']), reverse=True) == sorted(expected_rows, key=lambda x: (x['Provincia'], x['Kilometros_Totales']), reverse=True)


# 2. Test de top N ciclistas por provincia según el promedio diario de kilómetros recorridos
def test_promedio_diario_por_provincia(spark_session):
    # DataFrame intermedio con actividades de ciclistas
    df_actividades = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', '2024-10-01', 30.0),  
            (118090887, 'Juan Perez', 'San José', '2024-10-01', 40.0),  # Dos actividades el mismo día para Juan Perez
            (123456789, 'Maria Gomez', 'Heredia', '2024-10-01', 20.0),  
            (123456789, 'Maria Gomez', 'Heredia', '2024-10-03', 50.0),  # Dos actividades en diferente día para Maria Gomez
            (111222333, 'Carlos Mora', 'San José', '2024-10-01', 25.0),  
            (987654321, 'Isabella Cruz', 'Heredia', '2024-10-01', 40.0),  
            (135790246, 'Javier Diaz', 'San José', '2024-10-01', 90.0), 
            (102030405, 'Sofía Alvarado', 'San José', '2024-10-01', 60.0),  
            (123456780, 'Daniela López', 'Heredia', '2024-10-01', 30.0),  
            (123456780, 'Daniela López', 'Heredia', '2024-10-03', 20.0),  
            (102030406, 'Luis Hernández', 'Heredia', '2024-10-01', 55.0),  
            (102030407, 'María López', 'San José', '2024-10-02', 80.0),  
            (102030408, 'Andrés Pérez', 'Heredia', '2024-10-02', 70.0),  
            (102030409, 'Pedro Martínez', 'San José', '2024-10-01', 45.0),  
            (102030410, 'Lucía Gómez', 'Heredia', '2024-10-01', 50.0), 
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Fecha', 'Kilometros']
    )

    # Calcula el total de km y los días activos por ciclista
    df_total_km_dia = df_actividades.groupBy("Cedula", "Nombre", "Provincia") \
                                      .agg(_sum("Kilometros").alias("Total_Kilometros"),
                                           count("Fecha").alias("Dias_Activos"))

    # Calcula el promedio diario de km recorridos
    df_promedio = df_total_km_dia.withColumn("Promedio_Diario", 
                                              col("Total_Kilometros") / col("Dias_Activos"))

    # Ordena y selecciona el top 5 por promedio diario
    df_top_5 = df_promedio.orderBy("Promedio_Diario", ascending=False) \
                           .groupBy("Provincia") \
                           .agg(collect_list(col("Nombre")).alias("Top_Ciclistas"), 
                                collect_list(col("Promedio_Diario")).alias("Promedios")) \
                           .select("Provincia", "Top_Ciclistas", "Promedios")

    # Datos esperados para el top 5
    expected_ds = spark_session.createDataFrame(
        [
            ('San José', ['Javier Diaz', 'Juan Perez', 'Sofía Alvarado', 'Carlos Mora', 'Pedro Martínez'], [90.0, 70.0, 60.0, 25.0, 45.0]),
            ('Heredia', ['Maria Gomez', 'Isabella Cruz', 'Daniela López', 'Luis Hernández', 'Lucía Gómez'], [35.0, 40.0, 25.0, 55.0, 50.0])
        ],
        ['Provincia', 'Top_Ciclistas', 'Promedios']
    )

    actual_rows = [row.asDict() for row in df_top_5.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Provincia']) == sorted(expected_rows, key=lambda x: x['Provincia'])


if __name__ == "__main__":
    spark = spark_session()
    test_top_n_ciclistas_por_km(spark)
    test_promedio_diario_por_provincia(spark)
    print("todos los tests pasaron bien")


