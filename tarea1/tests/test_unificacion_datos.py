import pytest
from pyspark.sql import SparkSession

# En esta parte del codigo iniciamos el parkSession para las pruebas
@pytest.fixture(scope="session")
def spark_session():
    return SparkSession.builder \
        .appName("Test Ciclistas") \
        .getOrCreate()

# Simula la función de la union de los datos
def unir_datos(df_ciclistas, df_rutas, df_actividades):
    df_ciclistas_actividades = df_ciclistas.join(df_actividades, "Cedula", how="left")
    df_completo = df_ciclistas_actividades.join(df_rutas, "Codigo_Ruta", how="left")
    return df_completo

# 1. Verifica que se una correctamente los ciclistas, las actividades y las rutas
def test_union_correcta(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (123456789, 'Maria Gomez', 'Heredia')],
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
    
    df_resultado = unir_datos(df_ciclistas, df_rutas, df_actividades)
    assert df_resultado.count() == 1

# 2.Los ciclistas sin actividad deben aparecer con valores nulos en las columnas de actividades
def test_ciclistas_sin_actividad(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (123456789, 'Maria Gomez', 'Heredia')],
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
    
    df_resultado = unir_datos(df_ciclistas, df_rutas, df_actividades)
    
    assert df_resultado.filter(df_resultado.Cedula == 123456789).count() == 1
    assert df_resultado.filter(df_resultado.Cedula == 123456789).select('Codigo_Ruta').collect()[0][0] is None

# 3. Los ciclistas con el mismo nombre pero con diferentes cédulas deben manejarse correctamente sin juntarse 
def test_ciclistas_mismo_nombre_diferente_cedula(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (118090888, 'Juan Perez', 'San José')],
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
    
    df_resultado = unir_datos(df_ciclistas, df_rutas, df_actividades)
    
    assert df_resultado.filter(df_resultado.Cedula == 118090887).count() == 1
    assert df_resultado.filter(df_resultado.Cedula == 118090888).count() == 1

# 4. Verificacion de ciclistas con actividades repetidas en diferentes rutas
def test_ciclistas_actividades_repetidas(spark_session):
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
    
    df_resultado = unir_datos(df_ciclistas, df_rutas, df_actividades)
    
    assert df_resultado.filter(df_resultado.Cedula == 118090887).count() == 2

# 5. Verificación de ciclistas duplicados con actividades en la misma ruta
def test_ciclistas_duplicados_misma_ruta(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (118090888, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (1, 118090888, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )
    
    df_resultado = unir_datos(df_ciclistas, df_rutas, df_actividades)
    
    assert df_resultado.filter(df_resultado.Cedula == 118090887).count() == 1
    assert df_resultado.filter(df_resultado.Cedula == 118090888).count() == 1

# 6. Unión de ciclistas de diferentes provincias y sus actividades
def test_ciclistas_diferentes_provincias(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (123456789, 'Maria Lopez', 'Heredia')],
        ['Cedula', 'Nombre', 'Provincia']
    )
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10), (2, 'Ruta Heredia', 8)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (2, 123456789, '2024-10-02')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )
    
    df_resultado = unir_datos(df_ciclistas, df_rutas, df_actividades)
    
    assert df_resultado.filter(df_resultado.Cedula == 118090887).count() == 1
    assert df_resultado.filter(df_resultado.Cedula == 123456789).count() == 1

# 7. Verificación de fechas faltantes en las actividades
def test_fechas_faltantes(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, None)],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )
    
    df_resultado = unir_datos(df_ciclistas, df_rutas, df_actividades)
    
    assert df_resultado.filter(df_resultado.Fecha.isNull()).count() == 1

# 8. Unión correcta de rutas sin actividades asociadas
def test_rutas_sin_actividades(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10), (2, 'Ruta Fantasma', 7)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )
    
    df_resultado = unir_datos(df_ciclistas, df_rutas, df_actividades)
    
    assert df_resultado.filter(df_resultado.Codigo_Ruta == 2).count() == 1

# 9. Ciclistas que nunca hicieron actividad deben aparecer en el conjunto de datos final
def test_ciclistas_sin_actividad_ninguna(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (123456789, 'Maria Lopez', 'Heredia')],
        ['Cedula', 'Nombre', 'Provincia']
    )
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    df_actividades = spark_session.createDataFrame([], ['Codigo_Ruta', 'Cedula', 'Fecha'])
    
    df_resultado = unir_datos(df_ciclistas, df_rutas, df_actividades)
    
    assert df_resultado.filter(df_resultado.Cedula == 123456789).count() == 1
    assert df_resultado.filter(df_resultado.Cedula == 123456789).select('Codigo_Ruta').collect()[0][0] is None

# 10. Verificación de la unión de ciclistas con múltiples actividades en diferentes días
def test_ciclistas_multiples_actividades_dias(spark_session):
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (1, 118090887, '2024-10-02')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )
    
    df_resultado = unir_datos(df_ciclistas, df_rutas, df_actividades)
    
    assert df_resultado.filter(df_resultado.Cedula == 118090887).count() == 2

