import pytest
from pyspark.sql import SparkSession
from pyspark.sql import Row
import funciones2  # Importa las funciones desde el archivo funciones2.py


spark = SparkSession.builder.appName("PruebasMetricasConFecha").getOrCreate()


spark.sparkContext.setLogLevel("ERROR")

# 1. Prueba para verificar que el DataFrame de métricas tiene la estructura correcta (tres columnas)
def test_estructura_dataframe_metricas():
    metricas_data = [
        ("caja_con_mas_ventas", 1.0, "2024/10/16"),
        ("caja_con_menos_ventas", 3.0, "2024/10/16"),
        ("percentil_25_por_caja", 371490.0, "2024/10/16"),
        ("percentil_50_por_caja", 445860.0, "2024/10/16"),
        ("percentil_75_por_caja", 465663.0, "2024/10/16"),
        ("producto_mas_vendido_por_unidad", "leche", "2024/10/16"),
        ("producto_de_mayor_ingreso", "leche", "2024/10/16"),
    ]
    expected_df = spark.createDataFrame([Row(metrica=x[0], valor=x[1], fecha=x[2]) for x in metricas_data])
    
    # Genera el DataFrame usando la función
    df_metricas = funciones2.generar_dataframe_metricas_con_fecha(metricas_data, spark)

    # Comprueba que el DataFrame generado tiene las mismas filas que el esperado
    assert df_metricas.collect() == expected_df.collect()

# 2. Prueba para verificar que el DataFrame tiene los valores correctos (incluso con tipos diferentes)
def test_valores_dataframe_metricas():
    metricas_data = [
        ("caja_con_mas_ventas", 1.0, "2024/10/16"),
        ("caja_con_menos_ventas", 3.0, "2024/10/16"),
        ("percentil_25_por_caja", 371490.0, "2024/10/16"),
        ("percentil_50_por_caja", 445860.0, "2024/10/16"),
        ("percentil_75_por_caja", 465663.0, "2024/10/16"),
        ("producto_mas_vendido_por_unidad", "leche", "2024/10/16"),
        ("producto_de_mayor_ingreso", "leche", "2024/10/16"),
    ]
    expected_df = spark.createDataFrame([Row(metrica=x[0], valor=str(x[1]) if isinstance(x[1], str) else float(x[1]), fecha=x[2]) for x in metricas_data])
    
    # Genera el DataFrame usando la función
    df_metricas = funciones2.generar_dataframe_metricas_con_fecha(metricas_data, spark)

    # Comprueba que el DataFrame generado tiene los mismos valores que el esperado
    assert df_metricas.collect() == expected_df.collect()

# 3. Prueba para verificar que el DataFrame tiene los nombres de las columnas correctos
def test_nombres_columnas_dataframe():
    metricas_data = [
        ("caja_con_mas_ventas", 1.0, "2024/10/16"),
        ("caja_con_menos_ventas", 3.0, "2024/10/16"),
    ]
    df_metricas = funciones2.generar_dataframe_metricas_con_fecha(metricas_data, spark)

    assert df_metricas.columns == ["metrica", "valor", "fecha"]


# 4. Prueba para verificar el comportamiento con datos duplicados
def test_manejo_datos_duplicados():
    metricas_data = [
        ("caja_con_mas_ventas", 1.0, "2024/10/16"),
        ("caja_con_mas_ventas", 1.0, "2024/10/16"),
        ("producto_mas_vendido_por_unidad", "leche", "2024/10/16"),
        ("producto_mas_vendido_por_unidad", "leche", "2024/10/16"),
    ]
    expected_data = [
        ("caja_con_mas_ventas", 1.0, "2024/10/16"),
        ("producto_mas_vendido_por_unidad", "leche", "2024/10/16"),
    ]
    expected_df = spark.createDataFrame([Row(metrica=x[0], valor=x[1], fecha=x[2]) for x in expected_data])

    df_metricas = funciones2.generar_dataframe_metricas_con_fecha(metricas_data, spark).dropDuplicates()

    assert df_metricas.collect() == expected_df.collect()



# 5. Prueba para asegurarse de que el DataFrame maneja correctamente diferentes formatos de fecha
def test_manejo_formatos_fecha():
    metricas_data = [
        ("caja_con_mas_ventas", 1.0, "2024-10-16"),
        ("caja_con_menos_ventas", 3.0, "2024/10/16"),
    ]
    expected_data = [
        ("caja_con_mas_ventas", 1.0, "2024-10-16"),
        ("caja_con_menos_ventas", 3.0, "2024/10/16"),
    ]
    expected_df = spark.createDataFrame([Row(metrica=x[0], valor=x[1], fecha=x[2]) for x in expected_data])

    df_metricas = funciones2.generar_dataframe_metricas_con_fecha(metricas_data, spark)

    assert df_metricas.collect() == expected_df.collect()

