from pyspark.sql import SparkSession
from pyspark.sql.functions import sum as _sum, countDistinct, col, rank, round,trim
from pyspark.sql.window import Window


spark = SparkSession.builder.appName("Ciclistas_Top5").getOrCreate()

# Carga los CSV 
df_ciclistas = spark.read.csv('/src/archivo/ciclista.csv', header=False, inferSchema=True)\
    .toDF('Cedula', 'Nombre', 'Provincia')

df_rutas = spark.read.csv('/src/archivo/ruta.csv', header=False, inferSchema=True)\
    .toDF('Codigo_Ruta', 'Nombre_Ruta', 'Kilometros')

df_actividades = spark.read.csv('/src/archivo/actividad.csv', header=False, inferSchema=True)\
    .toDF('Codigo_Ruta', 'Cedula', 'Fecha')

# Quita los espacios en blanco 
df_ciclistas = df_ciclistas.withColumn('Cedula', col('Cedula').cast('string').trim())
df_ciclistas = df_ciclistas.withColumn('Nombre', col('Nombre').trim())

# Une los datos
df_merged = df_actividades.join(df_ciclistas, 'Cedula')\
                          .join(df_rutas, 'Codigo_Ruta')

# Calcula los km totales por cada ciclista
df_total_km = df_merged.groupBy('Cedula', 'Nombre', 'Provincia')\
                       .agg(_sum('Kilometros').alias('Kilometros_Totales'))

# Calcular los dias en las que hay actividad
df_dias_actividades = df_merged.groupBy('Cedula', 'Nombre', 'Provincia')\
                               .agg(countDistinct('Fecha').alias('Dias_Activos'))

# Une las tablas con km totales y dias donde haya actividad 
df_final = df_total_km.join(df_dias_actividades, ['Cedula', 'Nombre', 'Provincia'])\
                      .withColumn('Promedio_Diario', round(col('Kilometros_Totales') / col('Dias_Activos'), 2))

# Crear una ventana de partición por provincia para el ranking
windowSpec = Window.partitionBy('Provincia').orderBy(col('Kilometros_Totales').desc(), col('Promedio_Diario').desc())

# Obtiene el top 5 de ciclistas por provincia
df_top_5 = df_final.withColumn("rank", rank().over(windowSpec))\
                   .filter(col("rank") <= 5)\
                   .drop("rank")

# Muestra el top 5 de ciclistas por provincia
df_top_5.show(35)
