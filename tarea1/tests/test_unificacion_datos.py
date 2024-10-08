from joiner import join_dataframes


# 1. Unión correcta de datos de ciclistas, rutas y actividades
def test_union_correcta(spark_session):
    # Datos de los ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (123456789, 'Maria Gomez', 'Heredia')],
        ['Cedula', 'Nombre', 'Provincia']
    )
    
    # Datos de las rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)],
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    
    # Datos de las actividades
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Unión de los ciclistas con las actividades
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

    # Verificación de los resultados
    assert actual_ds.collect() == expected_ds.collect()


# 2. Verificación de los ciclistas sin actividad (las columnas de actividad deben aparecer como null)
def test_ciclistas_sin_actividad(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (123456789, 'Maria Gomez', 'Heredia')],
        ['Cedula', 'Nombre', 'Provincia']
    )
    
    # Datos de  las rutas
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

    # Verificación de los resultados
    assert actual_ds.collect() == expected_ds.collect()


# 3. Verificación de ciclistas con el mismo nombre pero diferente cédula (no deben unirse)
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


# 4. Verificación de ciclistas con actividades repetidas en diferentes rutas
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


# 5. Verificación de rutas sin actividades (deben aparecer con columnas null en actividad )
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


# 6. Verificación de ciclistas sin actividad en ninguna ruta
def test_ciclistas_sin_actividad_total(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José'), (123456789, 'Maria Lopez', 'Heredia')],
        ['Cedula', 'Nombre', 'Provincia']
    )
    
    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)]
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    
    # Sin actividades
    df_actividades = spark_session.createDataFrame([], ['Codigo_Ruta', 'Cedula', 'Fecha'])

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    
    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados: Ambos ciclistas deben aparecer con columnas de actividad nulas
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', None, None, None, None),
            (123456789, 'Maria Lopez', 'Heredia', None, None, None, None)
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de resultados
    assert actual_ds.collect() == expected_ds.collect()


# 7. Verificación de múltiples actividades en un solo día
def test_multiples_actividades_un_dia(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')]
        ['Cedula', 'Nombre', 'Provincia']
    )
    
    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)]
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    
    # Actividades en un solo día en dos rutas diferentes
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (2, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    
    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados: Juan Perez debe aparecer dos veces con diferentes actividades
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
            (118090887, 'Juan Perez', 'San José', 2, 'Ruta Las Cruces', 5.5, '2024-10-01')
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de resultados
    assert actual_ds.collect() == expected_ds.collect()


# 8. Verificación de actividades duplicadas
def test_actividades_duplicadas(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')]
        ['Cedula', 'Nombre', 'Provincia']
    )
    
    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)]
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    
    # Actividades duplicadas
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (1, 118090887, '2024-10-01')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    
    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados: Solo debe aparecer una actividad
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01')
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de resultados
    assert actual_ds.dropDuplicates().collect() == expected_ds.collect()


# 9. Verificación de datos faltantes en ciclistas
def test_datos_faltantes_ciclistas(spark_session):
    # Datos de ciclistas (con datos faltantes)
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, None, 'San José'), (123456789, 'Maria Lopez', None)],
        ['Cedula', 'Nombre', 'Provincia']
    )
    
    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)]
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    
    # Sin actividades
    df_actividades = spark_session.createDataFrame([], ['Codigo_Ruta', 'Cedula', 'Fecha'])

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    
    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados: Los datos faltantes deben mantenerse como nulos
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, None, 'San José', None, None, None, None),
            (123456789, 'Maria Lopez', None, None, None, None, None)
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de resultados
    assert actual_ds.collect() == expected_ds.collect()


# 10. Verificación de ciclistas con múltiples actividades en diferentes días
def test_ciclistas_multiples_actividades_dias(spark_session):
    # Datos de ciclistas
    df_ciclistas = spark_session.createDataFrame(
        [(118090887, 'Juan Perez', 'San José')]
        ['Cedula', 'Nombre', 'Provincia']
    )
    
    # Datos de rutas
    df_rutas = spark_session.createDataFrame(
        [(1, 'Ventolera Escazú', 10)]
        ['Codigo_Ruta', 'Nombre_Ruta', 'Kilometros']
    )
    
    # Actividades en diferentes días
    df_actividades = spark_session.createDataFrame(
        [(1, 118090887, '2024-10-01'), (1, 118090887, '2024-10-02')],
        ['Codigo_Ruta', 'Cedula', 'Fecha']
    )

    # Unión de ciclistas con actividades
    actual_ds = join_dataframes(df_ciclistas, df_actividades, ['Cedula'], ['Cedula'])
    
    # Unión del resultado con rutas
    actual_ds = join_dataframes(actual_ds, df_rutas, ['Codigo_Ruta'], ['Codigo_Ruta'])

    # Datos esperados: Juan Perez debe aparecer dos veces con actividades en días diferentes
    expected_ds = spark_session.createDataFrame(
        [
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-01'),
            (118090887, 'Juan Perez', 'San José', 1, 'Ventolera Escazú', 10, '2024-10-02')
        ],
        ['Cedula', 'Nombre', 'Provincia', 'Codigo_Ruta', 'Nombre_Ruta', 'Kilometros', 'Fecha']
    )

    # Verificación de resultados
    assert actual_ds.collect() == expected_ds.collect()

