from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, countDistinct, col, rank, desc
from pyspark.sql.window import Window

# Crea la sesión de Spark
spark = SparkSession.builder.appName("Ciclistas_Top5_Verificaciones").getOrCreate()

# Carga los CSV sin encabezados para ciclistas
def carga_datos():
    print("Cargando los archivos CSV...")
    
    # Cargar ciclistas sin encabezado
    df_ciclistas = spark.read.csv('/src/archivo/ciclista.csv', header=False, inferSchema=True)\
        .toDF('Cedula', 'Nombre', 'Provincia')  # Especificar manualmente los nombres de las columnas
    df_rutas = spark.read.csv('/src/archivo/ruta.csv', header=True, inferSchema=True)\
        .toDF('Codigo_Ruta', 'Nombre_Ruta', 'Kilometros')
    df_actividades = spark.read.csv('/src/archivo/actividad.csv', header=True, inferSchema=True)\
        .toDF('Codigo_Ruta', 'Cedula', 'Fecha')
    
    print("Archivos cargados.")
    df_ciclistas.show(5)
    df_rutas.show(5)
    df_actividades.show(5)
    
    return df_ciclistas, df_rutas, df_actividades

# Verifica si todos los ciclistas tienen actividades
def verificar_ciclistas_sin_actividades(df_ciclistas, df_actividades):
    print("Verificando ciclistas sin actividades...")
    df_ciclistas_sin_actividades = df_ciclistas.join(df_actividades, on="Cedula", how="left")\
                                               .filter(df_actividades['Fecha'].isNull())
    if df_ciclistas_sin_actividades.count() == 0:
        print("Todos los ciclistas tienen actividades registradas.")
    else:
        print("Ciclistas sin actividades:")
        df_ciclistas_sin_actividades.show()

# Verifica si hay duplicados en el archivo de actividades
def verificar_duplicados_actividades(df_actividades):
    print("Verificando duplicados en el archivo de actividades...")
    df_duplicados_actividades = df_actividades.groupBy("Cedula").count().filter(col("count") > 1)
    if df_duplicados_actividades.count() == 0:
        print("No hay duplicados en el archivo de actividades.")
    else:
        print("Duplicados en actividades encontrados:")
        df_duplicados_actividades.show()

# Une los datos
def union_datos(df_ciclistas, df_rutas, df_actividades):
    print("Uniendo datos de ciclistas, actividades y rutas...")
    df_merged = df_actividades.join(df_ciclistas, 'Cedula').join(df_rutas, 'Codigo_Ruta')
    print("Datos unidos:")
    df_merged.show(5)
    return df_merged

# Calcula los km totales
def calcula_kilometros(df_merged):
    print("Calculando kilómetros totales por ciclista...")
    df_total_km = df_merged.groupBy('Cedula', 'Nombre', 'Provincia')\
                           .agg(_sum('Kilometros').alias('Kilometros_Totales'))
    df_total_km.show(5)
    return df_total_km

# Calcula el promedio diario
def calcula_promedio(df_merged):
    print("Calculando días activos por ciclista...")
    df_dias_actividades = df_merged.groupBy('Cedula', 'Nombre', 'Provincia')\
                                   .agg(countDistinct('Fecha').alias('Dias_Activos'))
    df_dias_actividades.show(5)
    return df_dias_actividades

# Obtiene el top 5 de ciclistas por cada provincia
def top_5_ciclistas(df_final, criterios):
    print("Generando el ranking de los ciclistas...")
    
    windowSpec = Window.partitionBy('Provincia').orderBy(*[col(criterio).desc() for criterio in criterios])

    # Aplicar el ranking
    df_top_5 = df_final.withColumn("rank", rank().over(windowSpec))\
                       .filter(col("rank") <= 5)\
                       .drop("rank")
    
    print("Top 5 ciclistas por provincia:")
    df_top_5.show(35)
    return df_top_5

# Programa principal
def main():
    # Carga los datos
    df_ciclistas, df_rutas, df_actividades = carga_datos()
    
    # Verificar ciclistas sin actividades
    verificar_ciclistas_sin_actividades(df_ciclistas, df_actividades)
    
    # Verificar duplicados en actividades
    verificar_duplicados_actividades(df_actividades)

    # Unión de datos
    df_merged = union_datos(df_ciclistas, df_rutas, df_actividades)

    # Verificar cuántos ciclistas tienen actividades
    print("Conteo de ciclistas por provincia después de la unión:")
    df_merged.groupBy("Provincia").count().show()

    # Calcula los km totales
    df_total_km = calcula_kilometros(df_merged)

    # Calcula el promedio diario
    df_dias_actividades = calcula_promedio(df_merged)

    # Une los datos finales
    df_final = df_total_km.join(df_dias_actividades, ['Cedula', 'Nombre', 'Provincia'])\
                          .withColumn('Promedio_Diario', col('Kilometros_Totales') / col('Dias_Activos'))

    # Obtener el top 5 por km y promedio diario
    df_top_5 = top_5_ciclistas(df_final, ['Kilometros_Totales', 'Promedio_Diario'])

if __name__ == "__main__":
    main()
