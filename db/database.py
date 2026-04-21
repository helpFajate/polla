"""
Modulo de acceso a la base de datos.

Provee la configuracion de conexion a SQL Server mediante ODBC
y funciones utilitarias para la ejecucion de consultas.
"""

import pyodbc
import os


def get_db_connection():
    """
    Crea y retorna una conexion activa a la base de datos SQL Server.

    Utiliza autenticacion de Windows (Trusted_Connection) sobre el
    servidor local 'amaterasu\\siesa', base de datos 'Reportes'.
    El cifrado se deshabilita para compatibilidad con el entorno local.

    Returns:
        pyodbc.Connection: Conexion activa lista para ejecutar consultas.

    Raises:
        pyodbc.Error: Si el driver ODBC no esta disponible o los
                      parametros de conexion son incorrectos.
    """
    server = r'amaterasu\siesa'
    database = 'Reportes'

    conn_str = (
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={server};"
        f"DATABASE={database};"
        "Trusted_Connection=yes;"
        "Encrypt=no;"
        "TrustServerCertificate=yes;"
    )

    return pyodbc.connect(conn_str)


def query_db(query, args=(), one=False):
    """
    Ejecuta una consulta SQL y retorna los resultados como diccionarios.

    Abre una conexion, ejecuta la consulta con los argumentos provistos
    y cierra la conexion en el bloque finally para garantizar la
    liberacion del recurso independientemente del resultado.

    Para sentencias que no retornan filas (INSERT, UPDATE, DELETE),
    realiza el commit automaticamente y retorna None.

    Args:
        query (str): Sentencia SQL parametrizada a ejecutar.
        args (tuple): Argumentos posicionales para los parametros
                      de la consulta. Por defecto es una tupla vacia.
        one (bool): Si es True, retorna unicamente el primer registro
                    o None si no hay resultados. Si es False, retorna
                    la lista completa de registros. Por defecto es False.

    Returns:
        list[dict] | dict | None:
            - Lista de diccionarios si one=False y hay resultados.
            - Diccionario con el primer registro si one=True y hay resultados.
            - None si la consulta no retorna filas o el resultado esta vacio.
    """
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute(query, args)

        if cur.description is None:
            conn.commit()
            return None

        columns = [column[0] for column in cur.description]
        rv = [dict(zip(columns, row)) for row in cur.fetchall()]

        return (rv[0] if rv else None) if one else rv
    finally:
        cur.close()
        conn.close()