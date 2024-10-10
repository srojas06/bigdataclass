from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, countDistinct, col
from pyspark.sql.window import Window
from pyspark.sql.functions import rank

# Crea la sesión de Spark
spark = SparkSession.builder.appName("Ciclistas_Top5").getOrCreate()

# Carga los CSV 
def carga_datos():
    
    spark.catalog.clearCache()
    
    # Cargar los datos desde los archivos CSV
    df_ciclistas = spark.read.csv('/src/archivo/ciclista.csv', header=False, inferSchema=True)\
        .toDF('Cedula', 'Nombre', 'Provincia')
    df_rutas = spark.read.csv('/src/archivo/ruta.csv', header=False, inferSchema=True)\
        .toDF('Codigo_Ruta', 'Nombre_Ruta', 'Kilometros')
    df_actividades = spark.read.csv('/src/archivo/actividad.csv', header=False, inferSchema=True)\
        .toDF('Codigo_Ruta', 'Cedula', 'Fecha')
    
    return df_ciclistas, df_rutas, df_actividades

# Une los datos
def union_datos(df_ciclistas, df_rutas, df_actividades):
    # Asegurarse de que no haya uniones dobles o múltiples
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

# Obtiene el top 5 de ciclistas por cada provincia, considerando km y luego promedio diario
def top_5_ciclistas(df_final):
    windowSpec = Window.partitionBy('Provincia')\
                       .orderBy(col('Kilometros_Totales').desc(), col('Promedio_Diario').desc())
    
    df_top_5 = df_final.withColumn("rank", rank().over(windowSpec))\
                       .filter(col("rank") <= 5)\
                       .drop("rank")
    return df_top_5


def main():
  
    df_ciclistas, df_rutas, df_actividades = carga_datos()
  
    # Une los datos
    df_merged = union_datos(df_ciclistas, df_rutas, df_actividades)

    # Calcula los km totales
    df_total_km = calcula_kilometros(df_merged)

    # Calcula  el promedio diario
    df_dias_actividades = calcula_promedio(df_merged)

    # Une los datos finales
    df_final = df_total_km.join(df_dias_actividades, ['Cedula', 'Nombre', 'Provincia'])\
                          .withColumn('Promedio_Diario', col('Kilometros_Totales') / col('Dias_Activos'))

    # Obtener el top 5 por provincia
    df_top_5 = top_5_ciclistas(df_final)
    
   
    print("Top 5 ciclistas por provincia:")
    df_top_5.show(35)  

if __name__ == "__main__":
    main()
