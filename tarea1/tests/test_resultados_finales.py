from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, avg as _avg, count, col, struct, collect_list, expr, explode, countDistinct, row_number
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType
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

    # Se agrupa por ciclista y provincia, sumando los kilómetros
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

    # se calcula el total y promedio
    df_top_n = df_actividades.groupBy("Cedula", "Nombre", "Provincia") \
        .agg(F.sum("Kilometros").alias("Total_Kilometros"),
             F.countDistinct("Fecha").alias("Dias_Activos")) \
        .withColumn("Promedio_Diario", col("Total_Kilometros") / col("Dias_Activos")) \
        .withColumn("Rank", F.row_number().over(Window.partitionBy("Provincia").orderBy(F.desc("Promedio_Diario")))) \
        .filter(col("Rank") <= 3) \
        .groupBy("Provincia") \
        .agg(F.collect_list(struct("Rank", "Nombre", "Promedio_Diario")).alias("Top_Ciclistas")) \
        .orderBy("Provincia")

 
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

    actual_rows = [row.asDict() for row in df_top_n.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    for actual in actual_rows:
        actual['Top_Ciclistas'] = [(x[0], x[1], x[2]) for x in actual['Top_Ciclistas']]

    for expected in expected_rows:
        expected['Top_Ciclistas'] = [(x[0], x[1], x[2]) for x in expected['Top_Ciclistas']]

    assert sorted(actual_rows, key=lambda x: x['Provincia']) == sorted(expected_rows, key=lambda x: x['Provincia'])

# Test 3 caso de empate
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

    # Calcula total y promedio
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

# Test 4 que verifica el ranking si hay varias actividades en diferentes días
def test_ranking_por_kilometros_totales(spark_session):
    # se crea un DataFrame con 8 ciclistas por provincia para hacer el top 5
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

# Test 5 que verifica si no hay ciclistas
def test_cero_ciclistas(spark_session):
    schema = StructType([
        StructField('Cedula', IntegerType(), True),
        StructField('Nombre', StringType(), True),
        StructField('Provincia', StringType(), True),
        StructField('Fecha', StringType(), True),
        StructField('Kilometros', FloatType(), True)
    ])
    
    # Crea un DataFrame vacío para simular que no hay ciclistas
    df_actividades = spark_session.createDataFrame([], schema)

    # se calcula el top N (en este caso, 5)
    n = 5
    df_top_n = df_actividades.groupBy("Cedula", "Nombre", "Provincia") \
        .agg(F.sum("Kilometros").alias("Kilometros_Totales")) \
        .orderBy("Kilometros_Totales", ascending=False)

    df_top_n_filtered = df_top_n.groupBy("Provincia") \
                                  .agg(collect_list(struct("Cedula", "Nombre", "Kilometros_Totales")).alias("Top_Ciclistas")) \
                                  .withColumn("Top_Ciclistas", expr(f"slice(Top_Ciclistas, 1, {n})"))

    # Verifica que el DataFrame esté vacío
    assert df_top_n_filtered.count() == 0  # No debe haber ciclistas en el ranking
    
# Test 6 de un solo ciclista y dos actividades en diferentes dias
def test_un_solo_ciclista(spark_session):
    # Crear un DataFrame con un solo ciclista
    df_actividades = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', '2024-10-01', 50.0),
            (118090887, 'Juan Perez', 'San José', '2024-10-02', 60.0),  # Juan tiene dos días de actividad
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Fecha', 'Kilometros']
    )

    # Calcula total y promedio
    df_top_n = df_actividades.groupBy("Cedula", "Nombre", "Provincia") \
        .agg(F.sum("Kilometros").alias("Total_Kilometros"),
             F.countDistinct("Fecha").alias("Dias_Activos")) \
        .withColumn("Promedio_Diario", col("Total_Kilometros") / col("Dias_Activos")) \
        .withColumn("Rank", F.row_number().over(Window.partitionBy("Provincia").orderBy(F.desc("Promedio_Diario")))) \
        .filter(col("Rank") <= 5) \
        .groupBy("Provincia") \
        .agg(F.collect_list(struct("Rank", "Nombre", "Promedio_Diario")).alias("Top_Ciclistas")) \
        .orderBy("Provincia")

    print("Top ciclista por promedio diario con ranking:")
    df_top_n.show()

    # Datos esperados para el único ciclista con ranking
    expected_ds = spark_session.createDataFrame(
        [
            ('San José', [(1, 'Juan Perez', 55.0)])  # Promedio diario: (50 + 60) / 2 = 55
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

# Test 7 de verificar el ranking en todas las provincias 
def test_multiples_provincias(spark_session):
    # Crear un DataFrame con ciclistas de 7 provincias
    df_actividades = spark_session.createDataFrame(
        [
            # San José
            (118090887, 'Juan Perez', 'San José', '2024-10-01', 50.0),
            (118090888, 'Carlos Mora', 'San José', '2024-10-01', 30.0),
            (118090889, 'Javier Diaz', 'San José', '2024-10-01', 90.0),
            (118090890, 'Sofía Alvarado', 'San José', '2024-10-01', 60.0),
            (118090891, 'María López', 'San José', '2024-10-01', 80.0),
            # Heredia
            (123456789, 'Maria Gomez', 'Heredia', '2024-10-01', 20.0),
            (123456780, 'Isabella Cruz', 'Heredia', '2024-10-01', 40.0),
            (123456781, 'Luis Hernández', 'Heredia', '2024-10-01', 55.0),
            (123456782, 'Lucía Gómez', 'Heredia', '2024-10-01', 50.0),
            (123456783, 'Daniela López', 'Heredia', '2024-10-01', 25.0),
            # Alajuela
            (222222222, 'Diego Sánchez', 'Alajuela', '2024-10-01', 85.0),
            (222222223, 'Laura Méndez', 'Alajuela', '2024-10-01', 40.0),
            (222222224, 'Isabel Torres', 'Alajuela', '2024-10-01', 55.0),
            (222222225, 'Miguel Ángel', 'Alajuela', '2024-10-01', 30.0),
            (222222226, 'Ricardo Gómez', 'Alajuela', '2024-10-01', 60.0),
            # Cartago
            (333333333, 'José Martínez', 'Cartago', '2024-10-01', 70.0),
            (333333334, 'Ana Torres', 'Cartago', '2024-10-01', 50.0),
            (333333335, 'Pablo Pérez', 'Cartago', '2024-10-01', 45.0),
            (333333336, 'Sofia Méndez', 'Cartago', '2024-10-01', 20.0),
            (333333337, 'Diego Ruiz', 'Cartago', '2024-10-01', 30.0),
            # Guanacaste
            (444444444, 'Fernando Castro', 'Guanacaste', '2024-10-01', 60.0),
            (444444445, 'Sofía Morales', 'Guanacaste', '2024-10-01', 90.0),
            (444444446, 'Rafael López', 'Guanacaste', '2024-10-01', 80.0),
            (444444447, 'María González', 'Guanacaste', '2024-10-01', 55.0),
            (444444448, 'Laura Jiménez', 'Guanacaste', '2024-10-01', 50.0),
            # Puntarenas
            (555555555, 'Carlos Díaz', 'Puntarenas', '2024-10-01', 75.0),
            (555555556, 'Isabel Jiménez', 'Puntarenas', '2024-10-01', 80.0),
            (555555557, 'Diego Hernández', 'Puntarenas', '2024-10-01', 70.0),
            (555555558, 'María Salas', 'Puntarenas', '2024-10-01', 30.0),
            (555555559, 'Ana Rodríguez', 'Puntarenas', '2024-10-01', 40.0),
            # Limón
            (666666666, 'Lucía González', 'Limón', '2024-10-01', 65.0),
            (666666667, 'Pedro López', 'Limón', '2024-10-01', 45.0),
            (666666668, 'Carlos Arias', 'Limón', '2024-10-01', 50.0),
            (666666669, 'Andrea Pérez', 'Limón', '2024-10-01', 30.0),
            (666666670, 'Fernando Ruiz', 'Limón', '2024-10-01', 25.0),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Fecha', 'Kilometros']
    )

    # Se ordena y se filtra por el top 3
    df_top_n = df_actividades.withColumn("Rank", F.row_number().over(Window.partitionBy("Provincia").orderBy(F.desc("Kilometros")))) \
        .filter(col("Rank") <= 3) \
        .groupBy("Provincia") \
        .agg(F.collect_list(struct("Rank", "Nombre", "Kilometros")).alias("Top_Ciclistas")) \
        .orderBy("Provincia")

    print("Top 3 ciclistas por total de kilómetros:")
    df_top_n.show()

    # Datos esperados para el top 3 con ranking
    expected_ds = spark_session.createDataFrame(
        [
            ('San José', [(1, 'Javier Diaz', 90.0), (2, 'María López', 80.0), (3, 'Sofía Alvarado', 60.0)]),
            ('Heredia', [(1, 'Luis Hernández', 55.0), (2, 'Lucía Gómez', 50.0), (3, 'Isabella Cruz', 40.0)]),
            ('Alajuela', [(1, 'Diego Sánchez', 85.0), (2, 'Ricardo Gómez', 60.0), (3, 'Isabel Torres', 55.0)]),
            ('Cartago', [(1, 'José Martínez', 70.0), (2, 'Ana Torres', 50.0), (3, 'Pablo Pérez', 45.0)]),
            ('Guanacaste', [(1, 'Sofía Morales', 90.0), (2, 'Rafael López', 80.0), (3, 'Fernando Castro', 60.0)]),
            ('Puntarenas', [(1, 'Isabel Jiménez', 80.0), (2, 'Carlos Díaz', 75.0), (3, 'Diego Hernández', 70.0)]),
            ('Limón', [(1, 'Lucía González', 65.0), (2, 'Carlos Arias', 50.0), (3, 'Pedro López', 45.0)]),
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

#  Test 8 de duplicados
# Este caso simula un escenario donde se verifica que por un error externo Juan Pérez aparece dos veces en la base de datos
# No se considera un duplicado si son dos actividades realizadas en el mismo día.
def test_duplicados(spark_session):
    df_actividades = spark_session.createDataFrame(
        [
            # Juan Pérez hace dos actividades en el mismo día
            (118090887, 'Juan Perez', 'San José', '2024-10-01', 50.0),
            (118090887, 'Juan Perez', 'San José', '2024-10-01', 50.0),  # Duplicado (error externo)
            (118090888, 'Carlos Mora', 'San José', '2024-10-01', 30.0),
            (118090889, 'Javier Diaz', 'San José', '2024-10-01', 90.0),
            (118090890, 'Sofía Alvarado', 'San José', '2024-10-01', 60.0),
            (118090891, 'María López', 'San José', '2024-10-01', 80.0),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Fecha', 'Kilometros']
    )

    # Calcular total y crear ranking
    df_top_n = df_actividades.groupBy("Cedula", "Nombre", "Provincia") \
        .agg(F.sum("Kilometros").alias("Total_Kilometros")) \
        .withColumn("Rank", F.row_number().over(Window.partitionBy("Provincia").orderBy(F.desc("Total_Kilometros")))) \
        .filter(col("Rank") <= 3) \
        .groupBy("Provincia") \
        .agg(F.collect_list(struct("Rank", "Nombre", "Total_Kilometros")).alias("Top_Ciclistas")) \
        .orderBy("Provincia")

    print("Top 3 ciclistas por total de kilómetros:")
    df_top_n.show()

    # Datos esperados para el top 3 con ranking
    expected_ds = spark_session.createDataFrame(
        [
            ('San José', [(1, 'Juan Perez', 100.0), (2, 'Javier Diaz', 90.0), (3, 'María López', 80.0)]),
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

# Test #9 ciclistas sin actividades
def test_ciclistas_sin_actividades(spark_session):
    # Crear un DataFrame con ciclistas que no tienen actividades registradas
    df_actividades = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', None, 0.0),  # Sin actividades
            (118090888, 'Carlos Mora', 'San José', None, 0.0),  # Sin actividades
            (118090889, 'Javier Diaz', 'Heredia', None, 0.0),   # Sin actividades
            (118090890, 'Sofía Alvarado', 'Alajuela', None, 0.0), # Sin actividades
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Fecha', 'Kilometros']
    )

    # Intentar calcular el ranking, pero como no hay kilómetros, no debería haber resultados
    df_top_n = df_actividades.groupBy("Cedula", "Nombre", "Provincia") \
        .agg(F.sum("Kilometros").alias("Total_Kilometros")) \
        .filter(col("Total_Kilometros") > 0) \
        .groupBy("Provincia") \
        .agg(F.collect_list(struct("Cedula", "Nombre", "Total_Kilometros")).alias("Top_Ciclistas")) \
        .orderBy("Provincia")

    # Verificar que el resultado esté vacío
    assert df_top_n.isEmpty()  # No debe haber ciclistas en el ranking




#  Test #10 de múltiples actividades en dias diferentes
def test_actividades_por_ciclista(spark_session):
    # Crear un DataFrame con ciclistas y sus actividades
    df_actividades = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', '2024-10-01', 10.0),
            (118090887, 'Juan Perez', 'San José', '2024-10-02', 15.0),  # Total: 25
            (118090888, 'Carlos Mora', 'San José', '2024-10-01', 20.0),  # Total: 20
            (118090889, 'Javier Diaz', 'Heredia', '2024-10-01', 30.0),
            (118090889, 'Javier Diaz', 'Heredia', '2024-10-02', 25.0),  # Total: 55
            (118090890, 'Sofía Alvarado', 'Alajuela', '2024-10-01', 40.0),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Fecha', 'Kilometros']
    )

    # Calcular total de kilómetros por ciclista
    df_totales = df_actividades.groupBy("Cedula", "Nombre", "Provincia") \
        .agg(F.sum("Kilometros").alias("Total_Kilometros"))

    print("Total de kilómetros por ciclista:")
    df_totales.show()

    # Datos esperados
    expected_data = [
        (118090887, 'Juan Perez', 'San José', 25.0),
        (118090888, 'Carlos Mora', 'San José', 20.0),
        (118090889, 'Javier Diaz', 'Heredia', 55.0),
        (118090890, 'Sofía Alvarado', 'Alajuela', 40.0),
    ]

    expected_ds = spark_session.createDataFrame(expected_data, ['Cedula', 'Nombre', 'Provincia', 'Total_Kilometros'])

    # Comparar resultados
    actual_rows = [row.asDict() for row in df_totales.collect()]
    expected_rows = [row.asDict() for row in expected_ds.collect()]

    assert sorted(actual_rows, key=lambda x: x['Cedula']) == sorted(expected_rows, key=lambda x: x['Cedula'])



if __name__ == "__main__":
    spark = spark_session()
    test_top_n_ciclistas_por_km(spark)
    test_promedio_diario_por_provincia(spark)
    test_empates_en_kilometros(spark)
    test_ranking_por_kilometros_totales(spark)
    test_cero_ciclistas(spark)
    test_un_solo_ciclista(spark)
    test_multiples_provincias(spark)
    test_duplicados(spark)
    test_ciclistas_sin_actividades(spark)
    test_multiples_actividades_dias_diferentes(spark)

    
    print("Todos los tests pasaron correctamente.")
