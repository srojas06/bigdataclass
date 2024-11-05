from funciones import iniciar_spark, load_data, preprocess_census_data, preprocess_crimes_data, save_to_postgres

def cruzar_datasets(census_df, crimes_df):
    """
    Realiza un cruce (join) entre los DataFrames de censo y crímenes usando el nombre del estado como clave.
    """
    cruzado_df = census_df.join(crimes_df, census_df["State"] == crimes_df["state_name"], how="inner")
    return cruzado_df

if __name__ == "__main__":
    spark = iniciar_spark("MaterializePostgres")

    # Rutas de los archivos de datos
    census_path = "/src/data/acs2017_census_tract_data.csv"
    crimes_path = "/src/data/estimated_crimes_1979_2019.csv"
    
    # Cargar y preprocesar los datos
    census_df, crimes_df = load_data(spark, census_path, crimes_path)
    census_df = preprocess_census_data(census_df)
    crimes_df = preprocess_crimes_data(crimes_df)

    # Configuración de conexión a PostgreSQL
    jdbc_url = "jdbc:postgresql://localhost:5433/bigdata_db"
    properties = {
        "user": "postgres",
        "password": "testPassword",
        "driver": "org.postgresql.Driver"
    }
    
    # Guardar los datos preprocesados (antes del cruce) en PostgreSQL
    save_to_postgres(census_df, "census_data", jdbc_url, properties)
    save_to_postgres(crimes_df, "crimes_data", jdbc_url, properties)
    
    # Realizar el cruce y guardar el conjunto de datos cruzado en PostgreSQL
    cruzado_df = cruzar_datasets(census_df, crimes_df)
    save_to_postgres(cruzado_df, "crossed_data", jdbc_url, properties)

    spark.stop()
