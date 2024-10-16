import pytest
from pyspark.sql import SparkSession
import funciones  # Importar las funciones desde el archivo funciones.py
import pyspark.sql.functions as F

# Crear la sesión de Spark
spark = SparkSession.builder.appName("PruebasTotalCajas").getOrCreate()

# Deshabilitar los logs innecesarios de Spark
spark.sparkContext.setLogLevel("ERROR")

# 1. Prueba con múltiples cajas y ventas positivas
def test_total_cajas_multiples():
    data = [("caja1", 100.50), ("caja2", 150.75), ("caja1", 50.25)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    resultado = funciones.calcular_total_cajas(df)
    
    esperado = [("caja1", 150.75), ("caja2", 150.75)]
    
    assert resultado.collect() == esperado

# 2. Prueba con una caja sin ventas
def test_total_cajas_sin_ventas():
    data = [("caja1", 0), ("caja2", 0)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    resultado = funciones.calcular_total_cajas(df)
    
    esperado = [("caja1", 0), ("caja2", 0)]
    
    assert resultado.collect() == esperado

# 3. Prueba con ventas nulas para una caja específica
def test_total_cajas_con_nulos():
    data = [("caja1", 100), ("caja2", None), ("caja3", 50)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    resultado = funciones.calcular_total_cajas(df)
    
    esperado = [("caja1", 100), ("caja3", 50)]
    
    assert resultado.collect() == esperado

# 4. Prueba con devoluciones (ventas negativas deben ser ignoradas)
def test_total_cajas_con_devoluciones():
    data = [("caja1", 100), ("caja2", -50), ("caja3", 150)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    resultado = funciones.calcular_total_cajas(df)
    
    # Las ventas negativas (-50) deben ser ignoradas
    esperado = [("caja1", 100), ("caja3", 150)]
    
    assert resultado.collect() == esperado

# 5. Prueba con ventas de cero unidades
def test_total_cajas_con_ventas_cero():
    data = [("caja1", 0), ("caja2", 0), ("caja3", 0)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    resultado = funciones.calcular_total_cajas(df)
    
    esperado = [("caja1", 0), ("caja2", 0), ("caja3", 0)]
    
    assert resultado.collect() == esperado

# 6. Prueba con combinación de ventas positivas y negativas (negativas ignoradas)
def test_total_cajas_combinacion_ventas():
    data = [("caja1", 100), ("caja1", -20), ("caja2", 150)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    resultado = funciones.calcular_total_cajas(df)
    
    # La venta negativa de -20 debe ser ignorada para caja1
    esperado = [("caja1", 100), ("caja2", 150)]
    
    assert resultado.collect() == esperado

# 7. Prueba con cajas duplicadas
def test_total_cajas_duplicadas():
    data = [("caja1", 100), ("caja1", 200), ("caja2", 150)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    resultado = funciones.calcular_total_cajas(df)
    
    esperado = [("caja1", 300), ("caja2", 150)]
    
    assert resultado.collect() == esperado

# 8. Prueba con cajas sin nombre o identificador nulo
def test_total_cajas_identificador_nulo():
    data = [(None, 100), ("caja2", 150), ("caja3", 200)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    resultado = funciones.calcular_total_cajas(df)
    
    esperado = [("caja2", 150), ("caja3", 200)]
    
    assert resultado.collect() == esperado

# 9. Prueba con ventas con decimales
def test_total_cajas_con_decimales():
    data = [("caja1", 100.50), ("caja2", 150.75), ("caja3", 50.25)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    resultado = funciones.calcular_total_cajas(df)
    
    esperado = [("caja1", 100.50), ("caja2", 150.75), ("caja3", 50.25)]
    
    assert resultado.collect() == esperado

# 10. Prueba con ventas extremas (muy grandes y muy pequeñas)
def test_total_cajas_con_ventas_extremas():
    data = [("caja1", 1000000), ("caja2", 0.01), ("caja3", 500000)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    resultado = funciones.calcular_total_cajas(df)
    
    esperado = [("caja1", 1000000), ("caja2", 0.01), ("caja3", 500000)]
    
    assert resultado.collect() == esperado


# Cerrar la sesión de Spark al final de las pruebas
@pytest.fixture(scope="session", autouse=True)
def finalizar_spark():
    yield
    spark.stop()

