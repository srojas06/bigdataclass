from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, countDistinct, col, rank
from pyspark.sql.window import Window

# Crea la sesión de Spark
spark = SparkSession.builder.appName("Ciclistas_Top5").getOrCreate()

# Cargar los CSV
df_ciclistas = spark.read.csv('/src/archivo/ciclista.csv', header=False, inferSchema=True)\
    .toDF('Cedula', 'Nombre', 'Provincia')

df_rutas = spark.read.csv('/src/archivo/ruta.csv', header=False, inferSchema=True)\
    .toDF('Codigo_Ruta', 'Nombre_Ruta', 'Kilometros')

df_actividades = spark.read.csv('/src/archivo/actividad.csv', header=False, inferSchema=True)\
    .toDF('Codigo_Ruta', 'Cedula', 'Fecha')

# Unir los datos
df_merged = df_actividades.join(df_ciclistas, 'Cedula')\
                          .join(df_rutas, 'Codigo_Ruta')

# Calcular los km totales por ciclista
df_total_km = df_merged.groupBy('Cedula', 'Nombre', 'Provincia')\
                       .agg(_sum('Kilometros').alias('Kilometros_Totales'))

# Calcular los días activos
df_dias_actividades = df_merged.groupBy('Cedula', 'Nombre', 'Provincia')\
                               .agg(countDistinct('Fecha').alias('Dias_Activos'))

# Unir las tablas con kilómetros totales y días activos
df_final = df_total_km.join(df_dias_actividades, ['Cedula', 'Nombre', 'Provincia'])\
                      .withColumn('Promedio_Diario', col('Kilometros_Totales') / col('Dias_Activos'))

# Crear una ventana de partición por provincia para el ranking
windowSpec = Window.partitionBy('Provincia').orderBy(col('Kilometros_Totales').desc(), col('Promedio_Diario').desc())

# Obtener el top 5 de ciclistas por provincia
df_top_5 = df_final.withColumn("rank", rank().over(windowSpec))\
                   .filter(col("rank") <= 5)\
                   .drop("rank")

# Mostrar el top 5 de ciclistas por provincia
df_top_5.show(35)
