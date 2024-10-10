from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, countDistinct, col
from pyspark.sql.window import Window
from pyspark.sql.functions import rank


spark = SparkSession.builder.appName("Top5_Ciclistas_Provincia").getOrCreate()


def cargar_datos():
    df_ciclistas = spark.read.csv('/src/archivo/ciclista.csv', header=False, inferSchema=True)\
        .toDF('Cedula', 'Nombre', 'Provincia')
    df_rutas = spark.read.csv('/src/archivo/ruta.csv', header=False, inferSchema=True)\
        .toDF('Codigo_Ruta', 'Nombre_Ruta', 'Kilometros')
    df_actividades = spark.read.csv('/src/archivo/actividad.csv', header=False, inferSchema=True)\
        .toDF('Codigo_Ruta', 'Cedula', 'Fecha')
    return df_ciclistas, df_rutas, df_actividades


def unir_datos(df_ciclistas, df_rutas, df_actividades):
    df_union = df_actividades.join(df_ciclistas, 'Cedula')\
                             .join(df_rutas, 'Codigo_Ruta')
    return df_union


def calcular_kilometros_totales(df_union):
    df_total_km = df_union.groupBy('Cedula', 'Nombre', 'Provincia')\
                          .agg(_sum('Kilometros').alias('Kilometros_Totales'))
    return df_total_km


def calcular_promedio_diario(df_union):
    df_dias_activos = df_union.groupBy('Cedula', 'Nombre', 'Provincia')\
                              .agg(countDistinct('Fecha').alias('Dias_Activos'))
    return df_dias_activos

def obtener_top_5(df_final, criterio):
    windowSpec = Window.partitionBy('Provincia').orderBy(col(criterio).desc())
    df_top_5 = df_final.withColumn("rank", rank().over(windowSpec))\
                       .filter(col("rank") <= 5)\
                       .drop("rank")
    return df_top_5


def main():
    
    df_ciclistas, df_rutas, df_actividades = cargar_datos()

    # se une los datos
    df_union = unir_datos(df_ciclistas, df_rutas, df_actividades)

    # se calcular el total de km recorridos por ciclista
    df_total_km = calcular_kilometros_totales(df_union)

    # se calcula el promedio diario de kilómetros recorridos por ciclista
    df_promedio_diario = calcular_promedio_diario(df_union)

    # se une el total de km con el promedio diario
    df_final = df_total_km.join(df_promedio_diario, ['Cedula', 'Nombre', 'Provincia'])\
                          .withColumn('Promedio_Diario', col('Kilometros_Totales') / col('Dias_Activos'))

    # top 5 por km totales
    df_top_5_km = obtener_top_5(df_final, 'Kilometros_Totales')
    print("Top 5 ciclistas por provincia (kilómetros totales):")
    df_top_5_km.show()

    # se obtiene el top 5 por promedio diario
    df_top_5_promedio = obtener_top_5(df_final, 'Promedio_Diario')
    print("\nTop 5 ciclistas por provincia (promedio diario):")
    df_top_5_promedio.show()

if __name__ == "__main__":
    main()

