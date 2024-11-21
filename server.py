from flask import Flask, jsonify, request
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor

app = Flask(__name__)
CORS(app)  # Permitir todas las solicitudes de cualquier origen

# Configuración de conexión a la base de datos
DATABASE_CONFIG = {
    'dbname': 'restaurante_faqg',
    'user': 'restaurante_faqg_user',
    'password': 'zG7wpyfHXpoA3849PX0K3MigHwQJ2aal',
    'host': 'dpg-csv4mb3qf0us739ind90-a.oregon-postgres.render.com',
    'port': 5432
}

def conectar_bd():
    """Establece la conexión a la base de datos."""
    try:
        conexion = psycopg2.connect(**DATABASE_CONFIG, sslmode='require')
        return conexion
    except psycopg2.Error as e:
        print(f"Error al conectar a la base de datos: {e}")
        return None

@app.route('/')
def index():
    return jsonify({"message": "Servidor funcionando correctamente."})

@app.route('/clientes-frecuentes', methods=['GET'])
def clientes_frecuentes():
    """Consulta los tres clientes más frecuentes en los últimos meses."""
    query = """
    WITH clientes_frecuentes AS (
        SELECT 
            DATE_TRUNC('month', p.fecha) AS mes, 
            c.nombre, 
            c.apellidos, 
            COUNT(p.idCliente) AS total_pedidos
        FROM 
            pedidos p
        JOIN 
            clientes c ON p.idCliente = c.idCliente
        WHERE 
            p.fecha >= DATE_TRUNC('month', CURRENT_DATE) - INTERVAL '2 months'
        GROUP BY 
            mes, c.nombre, c.apellidos
    )
    SELECT 
        mes, 
        nombre, 
        apellidos, 
        total_pedidos
    FROM 
        clientes_frecuentes
    ORDER BY 
        mes DESC, 
        total_pedidos DESC
    LIMIT 9;
    """
    return ejecutar_consulta(query)

@app.route('/platillos-mas-consumidos', methods=['GET'])
def platillos_mas_consumidos():
    """Consulta los tres platillos más consumidos cada mes."""
    query = """
    WITH platillos_mas_consumidos AS (
        SELECT 
            DATE_TRUNC('month', p.fecha) AS mes,
            pl.nombrePlatillo,
            SUM(dp.cantidad) AS total_consumido
        FROM 
            detallePedido dp
        JOIN 
            pedidos p ON dp.idpedido = p.idPedido
        JOIN 
            platillos pl ON dp.idPlatillo = pl.idPlatillo
        GROUP BY 
            mes, pl.nombrePlatillo
    )
    SELECT 
        mes,
        nombrePlatillo,
        total_consumido
    FROM 
        (
            SELECT 
                mes,
                nombrePlatillo,
                total_consumido,
                ROW_NUMBER() OVER (PARTITION BY mes ORDER BY total_consumido DESC) AS rank
            FROM 
                platillos_mas_consumidos
        ) AS ranked
    WHERE 
        rank <= 3
    ORDER BY 
        mes DESC, total_consumido DESC;
    """
    return ejecutar_consulta(query)

@app.route('/mesa-mas-usada', methods=['GET'])
def mesa_mas_usada():
    """Consulta la mesa que más se ocupa."""
    query = """
    SELECT 
        m.numMesa, 
        COUNT(p.idmesa) AS total_usos
    FROM 
        pedidos p
    JOIN 
        mesa m ON p.idmesa = m.idmesa
    GROUP BY 
        m.numMesa
    ORDER BY 
        total_usos DESC
    LIMIT 1;
    """
    return ejecutar_consulta(query)

@app.route('/mes-mas-clientes', methods=['GET'])
def mes_mas_clientes():
    """Consulta el mes con más clientes."""
    query = """
    SELECT 
        DATE_TRUNC('month', fecha) AS mes, 
        COUNT(DISTINCT idCliente) AS total_clientes
    FROM 
        pedidos
    GROUP BY 
        mes
    ORDER BY 
        total_clientes DESC
    LIMIT 1;
    """
    return ejecutar_consulta(query)

def ejecutar_consulta(query):
    """Ejecuta una consulta en la base de datos y devuelve los resultados en formato JSON."""
    try:
        conexion = conectar_bd()
        if not conexion:
            return jsonify({"error": "No se pudo conectar a la base de datos."}), 500
        with conexion.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query)
            resultados = cursor.fetchall()
        conexion.close()
        return jsonify(resultados)
    except psycopg2.Error as e:
        print(f"Error al ejecutar consulta: {e}")
        return jsonify({"error": "Ocurrió un error al ejecutar la consulta."}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
