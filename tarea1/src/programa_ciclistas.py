from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, countDistinct, col
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Crea la sesión de Spark
spark = SparkSession.builder.appName("Ciclistas_Top5").getOrCreate()

# Carga los CSV 
def carga_datos():
    df_ciclistas = spark.read.csv('/src/bigdataclass/tarea1/data/ciclista.csv', header=False, inferSchema=True)\
        .toDF('Cedula', 'Nombre', 'Provincia')
    df_rutas = spark.read.csv('/src/bigdataclass/tarea1/data/ruta.csv', header=False, inferSchema=True)\
        .toDF('Codigo_Ruta', 'Nombre_Ruta', 'Kilometros')
    df_actividades = spark.read.csv('/src/bigdataclass/tarea1/data/actividad.csv', header=False, inferSchema=True)\
        .toDF('Codigo_Ruta', 'Cedula', 'Fecha')
    return df_ciclistas, df_rutas, df_actividades


# Une los datos
def union_datos(df_ciclistas, df_rutas, df_actividades):
    df_merged = df_actividades.join(df_ciclistas, 'Cedula')\
                              .join(df_rutas, 'Codigo_Ruta')
    return df_merged

# Calcula los km totales
def calcula_kilometros(df_merged):
    df_total_km = df_merged.groupBy('Cedula', 'Nombre', 'Provincia')\
                           .agg(_sum('Kilometros').alias('Kilometros_Totales'))
    return df_total_km

# Calcula el promedio diario
def calcula_promedio(df_merged):
    df_dias_actividades = df_merged.groupBy('Cedula', 'Nombre', 'Provincia')\
                                   .agg(countDistinct('Fecha').alias('Dias_Activos'))
    return df_dias_actividades

# Obtiene el top 5 de ciclistas por cada provincia
def top_5_ciclistas(df_final, criterio):
    windowSpec = Window.partitionBy('Provincia').orderBy(col(criterio).desc())
    df_top_5 = df_final.withColumn("rank", rank().over(windowSpec))\
                       .filter(col("rank") <= 5)\
                       .drop("rank")
    return df_top_5

# Programa principal
def main():
    # Carga los datos
    df_ciclistas, df_rutas, df_actividades = carga_datos()
  
    # Une los datos
    df_merged = union_datos(df_ciclistas, df_rutas, df_actividades)

    # Calcula los km totales
    df_total_km = calcula_kilometros(df_merged)

    # Calcular el promedio diario
    df_dias_actividades = calcula_promedio(df_merged)

    # Une los datos finales
    df_final = df_total_km.join(df_dias_actividades, ['Cedula', 'Nombre', 'Provincia'])\
                          .withColumn('Promedio_Diario', col('Kilometros_Totales') / col('Dias_Activos'))

    # Obtiene el top 5 por km
    df_top_5_km = top_5_ciclistas(df_final, 'Kilometros_Totales')
    print("Top 5 ciclistas por provincia (kilómetros totales):")
    df_top_5_km.show()

    # Obtiene el top 5 por promedio diario
    df_top_5_prom = top_5_ciclistas(df_final, 'Promedio_Diario')
    print("\nTop 5 ciclistas por provincia (promedio diario):")
    df_top_5_prom.show()

if __name__ == "__main__":
    main()

