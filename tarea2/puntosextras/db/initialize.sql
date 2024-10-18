-- Crear la base de datos si no existe
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = 'bigdata_db') THEN
        CREATE DATABASE bigdata_db;
    END IF;
END
$$;

-- Conectar a la base de datos


-- Crear la tabla para almacenar los datos de ventas por caja
CREATE TABLE IF NOT EXISTS ventas_cajas (
    id SERIAL PRIMARY KEY,
    numero_caja VARCHAR(50) NOT NULL,
    total_vendido DOUBLE PRECISION NOT NULL,
    fecha DATE
);

-- Crear la tabla para almacenar el total de productos vendidos
CREATE TABLE IF NOT EXISTS productos_vendidos (
    id SERIAL PRIMARY KEY,
    nombre_producto VARCHAR(255) NOT NULL,
    cantidad_total INT NOT NULL,
    fecha DATE
);

-- Crear la tabla para almacenar las métricas calculadas
CREATE TABLE IF NOT EXISTS metricas (
    id SERIAL PRIMARY KEY,
    metrica VARCHAR(255) NOT NULL,
    valor VARCHAR(255) NOT NULL,
    fecha DATE
);

-- Insertar algunos datos iniciales
INSERT INTO ventas_cajas (numero_caja, total_vendido, fecha) VALUES ('Caja 1', 50000.75, '2024-10-15') ON CONFLICT DO NOTHING;
INSERT INTO productos_vendidos (nombre_producto, cantidad_total, fecha) VALUES ('Jugo', 120, '2024-10-15') ON CONFLICT DO NOTHING;
INSERT INTO metricas (metrica, valor, fecha) VALUES ('caja_con_mas_ventas', 'Caja 1', '2024-10-15') ON CONFLICT DO NOTHING;
