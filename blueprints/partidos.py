"""
Blueprint de partidos.

Expone las rutas relacionadas con la visualizacion de partidos disponibles
para pronosticar. Aplica ajuste de zona horaria UTC-5 (Colombia) para
mostrar unicamente los partidos del dia actual y del dia siguiente.
"""

from flask import Blueprint, render_template
from flask_login import login_required, current_user
from db.database import get_db_connection

partidos_bp = Blueprint('partidos', __name__)


@partidos_bp.route('/')
@partidos_bp.route('/partidos')
@login_required
def listar_partidos():
    """
    Lista los partidos pendientes de los proximos dos dias.

    Consulta la tabla wc_partidos filtrando aquellos que:
    - No han sido marcados como finalizados (finalizado = 0).
    - Tienen fecha programada para hoy o manana, calculada en hora
      colombiana (UTC-5) mediante DATEADD sobre la fecha UTC almacenada.

    Cada fila incluye una etiqueta 'HOY' o 'MANANA' para facilitar
    la agrupacion en la plantilla.

    Returns:
        Renderizado de partidos.html con la lista de partidos como contexto.
    """
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("""
        SELECT
            id,               -- p[0]
            equipo_local,     -- p[1]
            equipo_visitante, -- p[2]
            fecha_hora,       -- p[3]
            fase,             -- p[4]
            api_fixture_id,   -- p[5]
            CASE
                WHEN CAST(DATEADD(HOUR, -5, fecha_hora) AS DATE) =
                     CAST(DATEADD(HOUR, -5, GETUTCDATE()) AS DATE)
                THEN 'HOY'
                ELSE 'MANANA'
            END AS dia        -- p[6]
        FROM wc_partidos
        WHERE
            finalizado = 0
            AND CAST(DATEADD(HOUR, -5, fecha_hora) AS DATE) BETWEEN
                CAST(DATEADD(HOUR, -5, GETUTCDATE()) AS DATE) AND
                CAST(DATEADD(DAY, 1, DATEADD(HOUR, -5, GETUTCDATE())) AS DATE)
        ORDER BY fecha_hora ASC
    """)

    partidos = cursor.fetchall()
    conn.close()

    return render_template('partidos.html', partidos=partidos)