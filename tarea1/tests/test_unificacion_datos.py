from .joiner import join_dataframes 

def test_union_correcta(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (123456789, 'Maria Gomez', 'Heredia')],
        ['Cedula', 'Nombre', 'Provincia']
    )
    
    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    
    # Datos de actividades
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    
    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
            (123456789, 'Maria Gomez', 'Heredia', None, None, None, None),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de resultados
    assert actual_ds.collect() == expected_ds.collect()


def test_ciclistas_sin_actividad(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (123456789, 'Maria Gomez', 'Heredia')],
        ['Cedula', 'Nombre', 'Provincia']
    )
    
    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    
    # Solo una actividad
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    
    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados: Maria Gomez no tiene actividad, su columna de actividad es nula
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
            (123456789, 'Maria Gomez', 'Heredia', None, None, None, None),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de resultados
    assert actual_ds.collect() == expected_ds.collect()


def test_ciclistas_mismo_nombre_diferente_cedula(spark_session):
    # Datos de ciclistas con mismo nombre pero diferente cédula
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (118090888, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )
    
    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    
    # Actividades para uno de los Juan Perez
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    
    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados: Ambos Juan Perez deben aparecer por separado
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
            (118090888, 'Juan Perez', 'San José', None, None, None, None),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de resultados
    assert actual_ds.collect() == expected_ds.collect()


def test_ciclistas_actividades_repetidas(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')],
        ['Cedula', 'Nombre', 'Provincia']
    )
    
    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10), (2, 'Ruta Las Cruces', 5.5)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    
    # Actividades en dos rutas diferentes
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (2, 118090887, '2024-10-02')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    
    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados: Juan Perez con dos actividades
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
            (118090887, 'Juan Perez', 'San José', 2, 'Ruta Las Cruces', 5.5, '2024-10-02'),
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de resultados
    assert actual_ds.collect() == expected_ds.collect()


def test_rutas_sin_actividades(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')]
        ['Cedula', 'Nombre', 'Provincia']
    )
    
    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10), (2, 'Ruta Las Cruces', 5.5)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    
    # Sin actividades registradas para la Ruta Las Cruces
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    
    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados: Solo la ruta Ventolera Escazú tendrá actividad
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
            (118090887, 'Juan Perez', 'San José', None, None, None, None)
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de resultados
    assert actual_ds.collect() == expected_ds.collect()
