from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, countDistinct, col, rank
from pyspark.sql.window import Window

# Crea la sesión de Spark
spark = SparkSession.builder.appName("Ciclistas_Top5").getOrCreate()

# Carga los CSV sin encabezados para ciclistas
def carga_datos():
    # Cargar ciclistas sin encabezado
    df_ciclistas = spark.read.csv('/src/archivo/ciclista.csv', header=False, inferSchema=True)\
        .toDF('Cedula', 'Nombre', 'Provincia')  # Especificar manualmente los nombres de las columnas
    df_rutas = spark.read.csv('/src/archivo/ruta.csv', header=True, inferSchema=True)\
        .toDF('Codigo_Ruta', 'Nombre_Ruta', 'Kilometros')
    df_actividades = spark.read.csv('/src/archivo/actividad.csv', header=True, inferSchema=True)\
        .toDF('Codigo_Ruta', 'Cedula', 'Fecha')
    
    # Mostrar algunas filas de cada dataset para verificar la carga
    print("Ciclistas:")
    df_ciclistas.show(5)
    print("Rutas:")
    df_rutas.show(5)
    print("Actividades:")
    df_actividades.show(5)
    
    return df_ciclistas, df_rutas, df_actividades

# Une los datos
def union_datos(df_ciclistas, df_rutas, df_actividades):
    # Unir actividades con ciclistas
    df_merged = df_actividades.join(df_ciclistas, 'Cedula')
    
    # Verificar si se unieron correctamente
    print("Actividades unidas con ciclistas:")
    df_merged.show(5)
    
    # Unir con las rutas
    df_merged = df_merged.join(df_rutas, 'Codigo_Ruta')
    
    # Verificar la unión final
    print("Unión final con rutas:")
    df_merged.show(5)
    
    return df_merged

# Calcula los km totales
def calcula_kilometros(df_merged):
    df_total_km = df_merged.groupBy('Cedula', 'Nombre', 'Provincia')\
                           .agg(_sum('Kilometros').alias('Kilometros_Totales'))
    
    # Verificar el cálculo de kilómetros totales
    print("Kilómetros totales por ciclista:")
    df_total_km.show(5)
    
    return df_total_km

# Calcula el promedio diario
def calcula_promedio(df_merged):
    df_dias_actividades = df_merged.groupBy('Cedula', 'Nombre', 'Provincia')\
                                   .agg(countDistinct('Fecha').alias('Dias_Activos'))
    
    # Verificar el cálculo de días activos
    print("Días activos por ciclista:")
    df_dias_actividades.show(5)
    
    return df_dias_actividades

# Obtiene el top 5 de ciclistas por cada provincia
def top_5_ciclistas(df_final, criterios):
    # Definir la ventana de partición por provincia y el orden según los criterios
    windowSpec = Window.partitionBy('Provincia').orderBy(*[col(criterio).desc() for criterio in criterios])

    # Aplicar la función rank y filtrar los que están dentro del top 5
    df_top_5 = df_final.withColumn("rank", rank().over(windowSpec))\
                       .filter(col("rank") <= 5)\
                       .drop("rank")
    
    # Verificar los ciclistas rankeados
    print("Top 5 ciclistas por provincia:")
    df_top_5.show(35)
    
    return df_top_5

# Programa principal
def main():
    # Carga los datos
    df_ciclistas, df_rutas, df_actividades = carga_datos()
  
    # Une los datos
    df_merged = union_datos(df_ciclistas, df_rutas, df_actividades)
    
    # Verificar cuántos ciclistas tienen actividades
    print("Conteo de ciclistas por provincia:")
    df_merged.groupBy("Provincia").count().show()

    # Calcula los km totales
    df_total_km = calcula_kilometros(df_merged)

    # Calcular el promedio diario
    df_dias_actividades = calcula_promedio(df_merged)

    # Une los datos finales
    df_final = df_total_km.join(df_dias_actividades, ['Cedula', 'Nombre', 'Provincia'])\
                          .withColumn('Promedio_Diario', col('Kilometros_Totales') / col('Dias_Activos'))

    # Obtiene el top 5 por km y promedio diario
    df_top_5 = top_5_ciclistas(df_final, ['Kilometros_Totales', 'Promedio_Diario'])
    print("Top 5 ciclistas por provincia (kilómetros totales y promedio diario):")
    df_top_5.show(35)

if __name__ == "__main__":
    main()
