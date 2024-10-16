import pytest
from pyspark.sql import SparkSession
import funciones  # Importar las funciones desde el archivo funciones.py
import pyspark.sql.functions as F

# Crear la sesión de Spark
spark = SparkSession.builder.appName("PruebasMetricas").getOrCreate()

# Deshabilitar los logs innecesarios de Spark
spark.sparkContext.setLogLevel("ERROR")

# 1. Prueba para la caja con más ventas
def test_caja_con_mas_ventas():
    data = [("caja1", 100), ("caja2", 200), ("caja3", 150)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    caja_con_mas_ventas = funciones.calcular_metricas(df)[0]
    
    assert caja_con_mas_ventas == "caja2"

# 2. Prueba para la caja con menos ventas
def test_caja_con_menos_ventas():
    data = [("caja1", 100), ("caja2", 200), ("caja3", 50)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    caja_con_menos_ventas = funciones.calcular_metricas(df)[1]
    
    assert caja_con_menos_ventas == "caja3"

# 3. Prueba del percentil 25
def test_percentil_25():
    data = [("caja1", 100), ("caja2", 200), ("caja3", 300), ("caja4", 400)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    percentil_25 = funciones.calcular_metricas(df)[2]
    
    assert percentil_25 == 175  # Esperado basado en los datos

# 4. Prueba del percentil 50 (mediana)
def test_percentil_50():
    data = [("caja1", 100), ("caja2", 200), ("caja3", 300), ("caja4", 400)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    percentil_50 = funciones.calcular_metricas(df)[3]
    
    assert percentil_50 == 250  # Esperado basado en los datos

# 5. Prueba del percentil 75
def test_percentil_75():
    data = [("caja1", 100), ("caja2", 200), ("caja3", 300), ("caja4", 400)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    percentil_75 = funciones.calcular_metricas(df)[4]
    
    assert percentil_75 == 325  # Esperado basado en los datos

# 6. Prueba para el producto más vendido por unidad
def test_producto_mas_vendido():
    data = [("manzana", 10, 5), ("pera", 15, 3), ("manzana", 5, 5)]
    df = spark.createDataFrame(data, ["nombre_producto", "cantidad", "precio_unitario"])
    
    producto_mas_vendido = funciones.calcular_productos(df)[0]
    
    assert producto_mas_vendido == "manzana"

# 7. Prueba para el producto de mayor ingreso
def test_producto_mayor_ingreso():
    data = [("manzana", 10, 5), ("pera", 15, 3), ("manzana", 5, 5)]
    df = spark.createDataFrame(data, ["nombre_producto", "cantidad", "precio_unitario"])
    
    producto_mayor_ingreso = funciones.calcular_productos(df)[1]
    
    assert producto_mayor_ingreso == "manzana"

# 8. Prueba con un solo producto
def test_metricas_un_solo_producto():
    data = [("caja1", 150)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    metricas = funciones.calcular_metricas(df)
    
    assert metricas[0] == "caja1"
    assert metricas[1] == "caja1"

# 9. Prueba con cajas duplicadas
def test_metricas_cajas_duplicadas():
    data = [("caja1", 100), ("caja1", 200), ("caja2", 150)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    caja_con_mas_ventas = funciones.calcular_metricas(df)[0]
    
    assert caja_con_mas_ventas == "caja1"

# 10. Prueba con cajas con ventas iguales
def test_metricas_cajas_con_ventas_iguales():
    data = [("caja1", 200), ("caja2", 200)]
    df = spark.createDataFrame(data, ["numero_caja", "total_venta"])
    
    metricas = funciones.calcular_metricas(df)
    
    assert metricas[0] in ["caja1", "caja2"]  # Puede ser cualquiera
    assert metricas[1] in ["caja1", "caja2"]  # Puede ser cualquiera

# Cerrar la sesión de Spark al final de las pruebas
@pytest.fixture(scope="session", autouse=True)
def finalizar_spark():
    yield
    spark.stop()

