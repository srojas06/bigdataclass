from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, countDistinct, col
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Crea la sesión de Spark
spark = SparkSession.builder.appName("Ciclistas_Top5").getOrCreate()

# Carga los CSV 
def carga_datos():
    spark.catalog.clearCache()  # Limpiar cualquier cache antes de cargar nuevos datos
    df_ciclistas = spark.read.csv('/src/archivo/ciclista.csv', header=False, inferSchema=True)\
        .toDF('Cedula', 'Nombre', 'Provincia')
    df_rutas = spark.read.csv('/src/archivo/ruta.csv', header=False, inferSchema=True)\
        .toDF('Codigo_Ruta', 'Nombre_Ruta', 'Kilometros')
    df_actividades = spark.read.csv('/src/archivo/actividad.csv', header=False, inferSchema=True)\
        .toDF('Codigo_Ruta', 'Cedula', 'Fecha')

    # Imprime algunos datos de muestra para verificar la carga
    print("Datos de Ciclistas:")
    df_ciclistas.show(5)
    print("Datos de Rutas:")
    df_rutas.show(5)
    print("Datos de Actividades:")
    df_actividades.show(5)

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

# Obtiene el top 5 de ciclistas por cada provincia, considerando kilómetros y luego promedio diario
def top_5_ciclistas(df_final):
    windowSpec = Window.partitionBy('Provincia')\
                       .orderBy(col('Kilometros_Totales').desc(), col('Promedio_Diario').desc())
    
    # Aplicar la ventana y el ranking
    df_top_5 = df_final.withColumn("rank", rank().over(windowSpec))\
                       .filter(col("rank") <= 5)\
                       .drop("rank")
    
    return df_top_5

# Programa principal
def main():
    # Cargar los datos asegurando que Spark no mantenga caché anterior
    df_ciclistas, df_rutas, df_actividades = carga_datos()
  
    # Unir los datos
    df_merged = union_datos(df_ciclistas, df_rutas, df_actividades)

    # Calcular los km totales
    df_total_km = calcula_kilometros(df_merged)

    # Calcular el promedio diario
    df_dias_actividades = calcula_promedio(df_merged)

    # Unir los datos finales
    df_final = df_total_km.join(df_dias_actividades, ['Cedula', 'Nombre', 'Provincia'])\
                          .withColumn('Promedio_Diario', col('Kilometros_Totales') / col('Dias_Activos'))

    # Verificar el DataFrame antes de filtrar los Top 5
    print("Datos completos antes de filtrar:")
    df_final.show(35, truncate=False)

    # Obtener el top 5 por provincia
    df_top_5 = top_5_ciclistas(df_final)
    
    # Forzar la visualización de los 35 resultados (5 por cada provincia)
    df_top_5.show(35, truncate=False)  # `truncate=False` para no truncar los resultados

if __name__ == "__main__":
    main()
