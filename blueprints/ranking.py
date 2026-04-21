"""
Blueprint de ranking.

Expone la vista publica del ranking de usuarios ordenados por puntos
acumulados. Agrega los puntos de todos los pronosticos calificados
mediante SUM e ISNULL para tratar valores nulos como cero.
"""

from flask import Blueprint, render_template
from db.database import get_db_connection

ranking_bp = Blueprint('ranking', __name__)


@ranking_bp.route('/ranking')
def ver_ranking():
    """
    Muestra el ranking general de usuarios por puntos acumulados.

    Realiza un LEFT JOIN entre wc_usuarios y wc_pronosticos para incluir
    a todos los usuarios, incluso quienes aun no tienen pronosticos
    calificados. Los puntos nulos se normalizan a cero con ISNULL antes
    de sumar.

    Los resultados se convierten a una lista de diccionarios para facilitar
    el acceso por nombre de columna en la plantilla.

    En caso de error de base de datos, retorna el mensaje de la excepcion
    directamente como respuesta de texto para facilitar el diagnostico
    en desarrollo.

    Returns:
        Renderizado de ranking.html con la lista de usuarios y sus puntos,
        o un string con el error SQL si la consulta falla.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        query = """
            SELECT
                u.nombre,
                SUM(ISNULL(p.puntos_ganados, 0)) AS puntos_total
            FROM wc_usuarios u
            LEFT JOIN wc_pronosticos p ON u.id = p.usuario_id
            GROUP BY u.nombre
            ORDER BY puntos_total DESC
        """

        cursor.execute(query)
        columnas = [column[0] for column in cursor.description]
        usuarios_ranking = [dict(zip(columnas, row)) for row in cursor.fetchall()]

        conn.close()
        return render_template('ranking.html', ranking=usuarios_ranking)

    except Exception as e:
        return f"ERROR REAL EN SQL: {str(e)}"