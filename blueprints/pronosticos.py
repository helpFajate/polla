"""
Blueprint de pronosticos.

Gestiona el almacenamiento y la consulta de los pronosticos realizados
por los usuarios sobre partidos activos. Utiliza la sentencia MERGE de
T-SQL para insertar o actualizar un pronostico segun si ya existe un
registro previo del mismo usuario para el mismo partido.
"""

from flask import Blueprint, render_template, request, redirect, url_for, flash
from flask_login import login_required, current_user
from db.database import get_db_connection

pronosticos_bp = Blueprint('pronosticos', __name__)


@pronosticos_bp.route('/guardar', methods=['POST'])
@login_required
def guardar_pronostico():
    """
    Guarda o actualiza el pronostico de un usuario para un partido.

    Recibe via POST el identificador del partido y los goles pronosticados
    para cada equipo. Ejecuta un MERGE en wc_pronosticos para actualizar
    el registro si ya existe, o insertarlo si es nuevo.

    En caso de error de base de datos, registra la excepcion en consola
    y notifica al usuario mediante un mensaje flash.

    Returns:
        Redireccion al listado de partidos.
    """
    partido_id = request.form.get('partido_id')
    goles_local = request.form.get('goles_local')
    goles_visitante = request.form.get('goles_visitante')
    usuario_id = current_user.id

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            MERGE INTO wc_pronosticos AS target
            USING (SELECT ? AS u_id, ? AS p_id) AS source
            ON (target.usuario_id = source.u_id AND target.partido_id = source.p_id)
            WHEN MATCHED THEN
                UPDATE SET pronostico_local = ?, pronostico_visitante = ?, creado_en = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (usuario_id, partido_id, pronostico_local, pronostico_visitante)
                VALUES (?, ?, ?, ?);
        """, (usuario_id, partido_id, goles_local, goles_visitante,
              usuario_id, partido_id, goles_local, goles_visitante))

        conn.commit()
        conn.close()
        flash('Pronostico guardado!', 'success')
    except Exception as e:
        print(f"Error al guardar: {e}")
        flash('Error al guardar.', 'danger')

    return redirect(url_for('partidos.listar_partidos'))


@pronosticos_bp.route('/mis-pronosticos')
@login_required
def mis_pronosticos():
    """
    Muestra el historial de pronosticos del usuario autenticado.

    Consulta wc_pronosticos uniendo con wc_partidos para obtener los
    nombres de los equipos y la fecha de cada partido. Los resultados
    se ordenan cronologicamente de manera descendente.

    En caso de error, redirige al listado de partidos sin exponer
    el detalle del fallo al usuario.

    Returns:
        Renderizado de mis_pronosticos.html con la lista de apuestas,
        o redireccion al listado de partidos si ocurre un error.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT
                p.equipo_local,           -- ap[0]
                p.equipo_visitante,       -- ap[1]
                pr.pronostico_local,      -- ap[2]
                pr.pronostico_visitante,  -- ap[3]
                pr.puntos_ganados,        -- ap[4]
                p.fecha_hora              -- ap[5]
            FROM wc_pronosticos pr
            JOIN wc_partidos p ON pr.partido_id = p.id
            WHERE pr.usuario_id = ?
            ORDER BY p.fecha_hora DESC
        """, (current_user.id,))

        mis_apuestas = cursor.fetchall()
        conn.close()
        return render_template('mis_pronosticos.html', apuestas=mis_apuestas)
    except Exception as e:
        print(f"Error en mis-pronosticos: {e}")
        return redirect(url_for('partidos.listar_partidos'))