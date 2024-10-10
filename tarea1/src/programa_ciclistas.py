from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, countDistinct, col, rank
from pyspark.sql.window import Window

# Crea la sesión de Spark
spark = SparkSession.builder.appName("Ciclistas_Top5").getOrCreate()

# Carga los CSV 
def carga_datos():
    df_ciclistas = spark.read.csv('/src/archivo/ciclista.csv', header=True, inferSchema=True)\
        .toDF('Cedula', 'Nombre', 'Provincia')
    df_rutas = spark.read.csv('/src/archivo/ruta.csv', header=True, inferSchema=True)\
        .toDF('Codigo_Ruta', 'Nombre_Ruta', 'Kilometros')
    df_actividades = spark.read.csv('/src/archivo/actividad.csv', header=True, inferSchema=True)\
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
def top_5_ciclistas(df_final, criterios):
    # Verifica las provincias disponibles en el dataset
    df_final.groupBy("Provincia").count().show()  # Muestra cuántos ciclistas hay por provincia

    # Definir la ventana de partición por provincia y el orden según los criterios
    windowSpec = Window.partitionBy('Provincia').orderBy(*[col(criterio).desc() for criterio in criterios])

    # Aplicar la función rank y filtrar los que están dentro del top 5
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

    # Verificar datos cargados
    df_merged.show(10)  # Verifica que los datos estén bien

    # Calcula los km totales
    df_total_km = calcula_kilometros(df_merged)

    # Calcular el promedio diario
    df_dias_actividades = calcula_promedio(df_merged)

    # Verifica el cálculo de kilómetros y días activos
    df_total_km.show()  # Verifica los kilómetros calculados
    df_dias_actividades.show()  # Verifica los días activos calculados

    # Une los datos finales
    df_final = df_total_km.join(df_dias_actividades, ['Cedula', 'Nombre', 'Provincia'])\
                          .withColumn('Promedio_Diario', col('Kilometros_Totales') / col('Dias_Activos'))

    # Verifica los datos finales antes del ranking
    df_final.show(10)

    # Obtiene el top 5 por km y promedio diario
    df_top_5 = top_5_ciclistas(df_final, ['Kilometros_Totales', 'Promedio_Diario'])
    print("Top 5 ciclistas por provincia (kilómetros totales y promedio diario):")
    df_top_5.show(35)  # Aumenta el número mostrado para que veas más filas

if __name__ == "__main__":
    main()
