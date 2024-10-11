from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, avg as _avg, count, col, struct, collect_list, expr, explode,countDistinct, row_number
import pytest
from pyspark.sql import functions as F
from pyspark.sql import Window


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

    # se agrupa por ciclista y provincia, sumando los kilómetros
    df_top_n = df_merged.groupBy("Cedula", "Nombre", "Provincia") \
                         .agg(_sum("Kilometros").alias("Kilometros_Totales")) \
                         .orderBy("Provincia", "Kilometros_Totales", ascending=False)

    # se obtiene el top N (en este caso, top 5)
    n = 5
    df_top_n_filtered = df_top_n.groupBy("Provincia") \
                                  .agg(collect_list(struct("Cedula", "Nombre", "Kilometros_Totales")).alias("Top_Ciclistas")) \
                                  .withColumn("Top_Ciclistas", expr(f"slice(Top_Ciclistas, 1, {n})"))

    # se ordena el resultado para que el ciclista con mayor km esté primero
    df_top_n_sorted = df_top_n_filtered.select(
        "Provincia", 
        explode("Top_Ciclistas").alias("Ciclista")
    ).select(
        "Provincia",
        "Ciclista.Cedula",
        "Ciclista.Nombre",
        "Ciclista.Kilometros_Totales"
    ).orderBy("Provincia", "Ciclista.Kilometros_Totales", ascending=False)

    # datos esperados
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


# Test 2 de promedio diario por provincia y vamos hacer el top 3 de mejores ciclistas basandonos en el promedio diario
def test_promedio_diario_por_provincia(spark_session):
    # Crear un DataFrame con 5 ciclistas por provincia
    df_actividades = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', '2024-10-01', 30.0),
            (118090888, 'Carlos Mora', 'San José', '2024-10-01', 25.0),
            (118090889, 'Javier Diaz', 'San José', '2024-10-01', 90.0),
            (118090890, 'Sofía Alvarado', 'San José', '2024-10-01', 60.0),
            (118090891, 'María López', 'San José', '2024-10-01', 80.0),
            (123456789, 'Maria Gomez', 'Heredia', '2024-10-01', 20.0),
            (123456780, 'Isabella Cruz', 'Heredia', '2024-10-01', 40.0),
            (123456781, 'Luis Hernández', 'Heredia', '2024-10-01', 55.0),
            (123456782, 'Lucía Gómez', 'Heredia', '2024-10-01', 50.0),
            (123456783, 'Daniela López', 'Heredia', '2024-10-01', 25.0)
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Fecha', 'Kilometros']
    )

    # Calcular total y promedio
    df_top_n = df_actividades.groupBy("Cedula", "Nombre", "Provincia") \
        .agg(F.sum("Kilometros").alias("Total_Kilometros"),
             F.countDistinct("Fecha").alias("Dias_Activos")) \
        .withColumn("Promedio_Diario", col("Total_Kilometros") / col("Dias_Activos")) \
        .withColumn("Rank", F.row_number().over(Window.partitionBy("Provincia").orderBy(F.desc("Promedio_Diario")))) \
        .filter(col("Rank") <= 3) \
        .groupBy("Provincia") \
        .agg(F.collect_list(struct("Rank", "Nombre", "Promedio_Diario")).alias("Top_Ciclistas")) \
        .orderBy("Provincia")

    # Mostrar resultados
    print("Top 3 ciclistas por promedio diario con ranking:")
    df_top_n.show()

    # Datos esperados para el top 3 con ranking
    expected_ds = spark_session.createDataFrame(
        [
            ('San José', [(1, 'Javier Diaz', 90.0), (2, 'María López', 80.0), (3, 'Sofía Alvarado', 60.0)]),
            ('Heredia', [(1, 'Luis Hernández', 55.0), (2, 'Lucía Gómez', 50.0), (3, 'Isabella Cruz', 40.0)])
        ],
        ['Provincia', 'Top_Ciclistas']
    )

    # Comparar resultados
    actual_rows = [row.asDict() for row in df_top_n.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    # Asegúrate de que el formato sea correcto
    for actual in actual_rows:
        actual['Top_Ciclistas'] = [(x[0], x[1], x[2]) for x in actual['Top_Ciclistas']]

    for expected in expected_rows:
        expected['Top_Ciclistas'] = [(x[0], x[1], x[2]) for x in expected['Top_Ciclistas']]

    # Comparar resultados
    assert sorted(actual_rows, key=lambda x: x['Provincia']) == sorted(expected_rows, key=lambda x: x['Provincia'])

#Test 3 caso de empate
def test_empates_en_kilometros(spark_session):
    # Crear un DataFrame con datos de ciclistas que incluye un empate
    df_empates = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', '2024-10-01', 40.0),  # Actividad 1
            (118090887, 'Juan Perez', 'San José', '2024-10-02', 20.0),  # Actividad 2
            (118090888, 'Carlos Mora', 'San José', '2024-10-01', 40.0),  # Actividad 1
            (118090888, 'Carlos Mora', 'San José', '2024-10-02', 30.0)   # Actividad 2
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Fecha', 'Kilometros']
    )

    # Calcular total y promedio
    df_top_n = df_empates.groupBy("Cedula", "Nombre", "Provincia") \
                          .agg(F.sum("Kilometros").alias("Total_Kilometros"),
                               F.countDistinct("Fecha").alias("Dias_Activos")) \
                          .withColumn("Promedio_Diario", col("Total_Kilometros") / col("Dias_Activos")) \
                          .withColumn("Rank", F.row_number().over(Window.partitionBy("Provincia").orderBy(F.desc("Promedio_Diario")))) \
                          .filter(col("Rank") <= 5) \
                          .groupBy("Provincia") \
                          .agg(F.collect_list(struct("Rank", "Nombre", "Promedio_Diario")).alias("Top_Ciclistas")) \
                          .orderBy("Provincia")

 
    print("Top ciclistas por promedio diario con ranking:")
    df_top_n.show()

    # Datos esperados para el top con ranking
    expected_ds = spark_session.createDataFrame(
        [
            ('San José', [(1, 'Carlos Mora', 35.0), (2, 'Juan Perez', 30.0)]),  # Carlos tiene un promedio mayor
        ],
        ['Provincia', 'Top_Ciclistas']
    )

    actual_rows = [row.asDict() for row in df_top_n.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    for actual in actual_rows:
        actual['Top_Ciclistas'] = [(x[0], x[1], x[2]) for x in actual['Top_Ciclistas']]

    for expected in expected_rows:
        expected['Top_Ciclistas'] = [(x[0], x[1], x[2]) for x in expected['Top_Ciclistas']]

    assert sorted(actual_rows, key=lambda x: x['Provincia']) == sorted(expected_rows, key=lambda x: x['Provincia'])
    
#Test 4 que verifica el ranking  si hay varias actividades en diferentes dias
def test_ranking_por_kilometros_totales(spark_session):
    # Crear un DataFrame con 8 ciclistas por provincia para hacer el top 5
    df_actividad = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', '2024-10-01', 30.0),
            (118090887, 'Juan Perez', 'San José', '2024-10-02', 40.0),  # Total: 70
            (118090888, 'Carlos Mora', 'San José', '2024-10-01', 20.0),
            (118090888, 'Carlos Mora', 'San José', '2024-10-02', 10.0),  # Total: 30
            (118090889, 'Javier Diaz', 'San José', '2024-10-01', 90.0),  # Total: 90
            (118090890, 'Sofía Alvarado', 'San José', '2024-10-01', 60.0),  # Total: 60
            (118090891, 'María López', 'San José', '2024-10-01', 80.0),  # Total: 80
            (118090892, 'Pedro González', 'San José', '2024-10-01', 50.0),  # Total: 50
            (123456789, 'Maria Gomez', 'Heredia', '2024-10-01', 20.0),
            (123456789, 'Maria Gomez', 'Heredia', '2024-10-02', 10.0),  # Total: 30
            (123456780, 'Isabella Cruz', 'Heredia', '2024-10-01', 40.0),
            (123456780, 'Isabella Cruz', 'Heredia', '2024-10-02', 20.0),  # Total: 60
            (123456781, 'Luis Hernández', 'Heredia', '2024-10-01', 55.0),  # Total: 55
            (123456782, 'Lucía Gómez', 'Heredia', '2024-10-01', 50.0),  # Total: 50
            (123456783, 'Daniela López', 'Heredia', '2024-10-01', 25.0),  # Total: 25
            (123456784, 'Fernando Ruiz', 'Heredia', '2024-10-01', 65.0),  # Total: 65
            (123456785, 'Andrea Pérez', 'Heredia', '2024-10-01', 75.0),  # Total: 75
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Fecha', 'Kilometros']
    )

    # Calcula el total de km
    df_top_n = df_actividad.groupBy("Cedula", "Nombre", "Provincia") \
        .agg(F.sum("Kilometros").alias("Total_Kilometros")) \
        .withColumn("Rank", F.row_number().over(Window.partitionBy("Provincia").orderBy(F.desc("Total_Kilometros")))) \
        .filter(col("Rank") <= 5) \
        .groupBy("Provincia") \
        .agg(F.collect_list(struct("Rank", "Nombre", "Total_Kilometros")).alias("Top_Ciclistas")) \
        .orderBy("Provincia")


    print("Top 5 ciclistas por kilómetros totales con ranking:")
    df_top_n.show()

    # Datos esperados para el ranking
    expected_ds = spark_session.createDataFrame(
        [
            ('San José', [(1, 'Javier Diaz', 90.0), (2, 'María López', 80.0), (3, 'Juan Perez', 70.0), (4, 'Sofía Alvarado', 60.0), (5, 'Pedro González', 50.0)]),
            ('Heredia', [(1, 'Andrea Pérez', 75.0), (2, 'Fernando Ruiz', 65.0), (3, 'Isabella Cruz', 60.0), (4, 'Luis Hernández', 55.0), (5, 'Lucía Gómez', 50.0)])
        ],
        ['Provincia', 'Top_Ciclistas']
    )

    actual_rows = [row.asDict() for row in df_top_n.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

 
    for actual in actual_rows:
        actual['Top_Ciclistas'] = [(x[0], x[1], x[2]) for x in actual['Top_Ciclistas']]

    for expected in expected_rows:
        expected['Top_Ciclistas'] = [(x[0], x[1], x[2]) for x in expected['Top_Ciclistas']]


    assert sorted(actual_rows, key=lambda x: x['Provincia']) == sorted(expected_rows, key=lambda x: x['Provincia'])




if __name__ == "__main__":
    spark = spark_session()
    test_top_n_ciclistas_por_km(spark)
    test_promedio_diario_por_provincia(spark)
    test_empates_en_kilometros(spark)
    test_ranking_por_kilometros_totales(spark)
    print("Todos los tests pasaron correctamente.")
