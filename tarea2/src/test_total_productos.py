import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
import funciones  # Importa las funciones desde el archivo funciones.py

# Crea la sesión de Spark
spark = SparkSession.builder.appName("PruebasTotalProductos").getOrCreate()

# Deshabilita los logs innecesarios de Spark
spark.sparkContext.setLogLevel("ERROR")

# 1. Prueba con múltiples productos
def test_total_productos_multiples():
    data = [("manzana", 10), ("pera", 8), ("pasta", 12), ("manzana", 5), ("pera", 7)]
    df = spark.createDataFrame(data, ["nombre_producto", "cantidad"])
    
    resultado = funciones.calcular_total_productos(df)
    
    esperado = [("manzana", 15), ("pera", 15), ("pasta", 12)]
    
    assert resultado.collect() == esperado

# 2. Prueba sin productos (escenario vacío)
def test_total_productos_sin_productos():
    # Definimos el esquema para un DataFrame vacío
    schema = StructType([
        StructField("nombre_producto", StringType(), True),
        StructField("cantidad", IntegerType(), True)
    ])
    df = spark.createDataFrame([], schema=schema)
    
    resultado = funciones.calcular_total_productos(df)
    
    esperado = []
    assert resultado.collect() == esperado

# 3. Prueba con un solo producto
def test_total_productos_un_solo_producto():
    data = [("manzana", 10)]
    df = spark.createDataFrame(data, ["nombre_producto", "cantidad"])
    
    resultado = funciones.calcular_total_productos(df)
    
    esperado = [("manzana", 10)]
    
    assert resultado.collect() == esperado

# 4. Prueba con productos duplicados
def test_total_productos_duplicados():
    data = [("manzana", 10), ("manzana", 15), ("manzana", 5)]
    df = spark.createDataFrame(data, ["nombre_producto", "cantidad"])
    
    resultado = funciones.calcular_total_productos(df)
    
    esperado = [("manzana", 30)]
    
    assert resultado.collect() == esperado

# 5. Prueba con nombres similares (mayúsculas y minúsculas unificados)
def test_total_productos_unificar_mayusculas_minusculas():
    data = [("manzana", 10), ("Manzana", 5), ("MANZANA", 3)]
    df = spark.createDataFrame(data, ["nombre_producto", "cantidad"])
    
    # Llamamos a la función que ya convierte los nombres a minúsculas
    resultado = funciones.calcular_total_productos(df)
    
    # Todos los casos de "manzana" en distintas mayúsculas y minúsculas deben ser tratados como uno solo
    esperado = [("manzana", 18)]  # Se contabilizan como un solo producto
    
    assert resultado.collect() == esperado

# 6. Prueba con cantidades negativas (devoluciones ignoradas)
def test_total_productos_cantidades_negativas():
    data = [("manzana", 10), ("pera", -3), ("pasta", 5)]
    df = spark.createDataFrame(data, ["nombre_producto", "cantidad"])
    
    resultado = funciones.calcular_total_productos(df)
    
    # Las cantidades negativas son ignoradas
    esperado = [("manzana", 10), ("pasta", 5)]
    
    assert resultado.collect() == esperado

# 7. Prueba con valores nulos
def test_total_productos_con_nulos():
    data = [("manzana", 10), (None, 5), ("pera", None)]
    df = spark.createDataFrame(data, ["nombre_producto", "cantidad"])

    resultado = funciones.calcular_total_productos(df)

    # Solo se debe considerar la manzana, ya que los valores nulos deben ser ignorados
    esperado = [("manzana", 10)]

    assert resultado.collect() == esperado

# 8. Prueba con cantidades cero
def test_total_productos_con_cantidades_cero():
    data = [("manzana", 10), ("pera", 0), ("naranja", 0)]
    df = spark.createDataFrame(data, ["nombre_producto", "cantidad"])

    resultado = funciones.calcular_total_productos(df)

    esperado = [("manzana", 10)]

    assert resultado.collect() == esperado

# 9. Prueba con nombres de productos largos
def test_total_productos_nombres_largos():
    data = [("manzana_extra_larga_con_muchos_caracteres", 10), ("pera", 5)]
    df = spark.createDataFrame(data, ["nombre_producto", "cantidad"])
    
    resultado = funciones.calcular_total_productos(df)
    
    esperado = [("manzana_extra_larga_con_muchos_caracteres", 10), ("pera", 5)]
    
    assert resultado.collect() == esperado

# 10. Prueba con cantidades decimales
def test_total_productos_con_decimales():
    data = [("manzana", 1.5), ("pera", 2.3), ("manzana", 0.5)]
    df = spark.createDataFrame(data, ["nombre_producto", "cantidad"])
    
    resultado = funciones.calcular_total_productos(df)
    
    esperado = [("manzana", 2.0), ("pera", 2.3)]
    
    assert resultado.collect() == esperado

# Cerrar la sesión de Spark 
@pytest.fixture(scope="session", autouse=True)
def finalizar_spark():
    yield
    spark.stop()
