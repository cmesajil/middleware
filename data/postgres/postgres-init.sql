CREATE TABLE mensajes_correo (
    id_correo SERIAL PRIMARY KEY,
    id_usuario INT,
    asunto VARCHAR(255),
    cuerpo TEXT,
    fecha_envio TIMESTAMP,
    tipo_correo VARCHAR(50),
    prioridad INT
);

-- Ejecutar primero para crear la BD:
-- CREATE DATABASE bd1_texto;

CREATE TABLE mensajes_texto (
    id_mensaje SERIAL PRIMARY KEY, -- Se cambió AUTO_INCREMENT por SERIAL
    id_usuario INT,
    contenido TEXT,
    fecha_envio TIMESTAMP,         -- Se cambió DATETIME por TIMESTAMP
    tipo_mensaje VARCHAR(50),
    canal VARCHAR(50)
);

-- Ejecutar primero para crear la BD:
-- CREATE DATABASE bd3_prestamos;

CREATE TABLE usuarios (
    id_usuario SERIAL PRIMARY KEY,
    nombre VARCHAR(100),
    dni VARCHAR(20),
    correo VARCHAR(100),
    telefono VARCHAR(20),
    direccion TEXT,
    fecha_registro TIMESTAMP
);

CREATE TABLE cuentas (
    id_cuenta SERIAL PRIMARY KEY,
    id_usuario INT,
    saldo DECIMAL(10,2),
    estado VARCHAR(50),
    fecha_apertura TIMESTAMP,
    tipo_cuenta VARCHAR(50),
    CONSTRAINT fk_usuario_cuentas FOREIGN KEY (id_usuario) REFERENCES usuarios(id_usuario)
);

CREATE TABLE prestamos (
    id_prestamo SERIAL PRIMARY KEY,
    id_usuario INT,
    monto DECIMAL(10,2),
    tasa_interes DECIMAL(5,2),
    plazo INT,
    estado VARCHAR(50),
    fecha_inicio TIMESTAMP,
    fecha_fin TIMESTAMP,
    tipo_prestamo VARCHAR(50),
    CONSTRAINT fk_usuario_prestamos FOREIGN KEY (id_usuario) REFERENCES usuarios(id_usuario)
);
