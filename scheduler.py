"""
Modulo de tareas programadas.

Centraliza la logica de sincronizacion con la API de football y el
calculo de puntos. Este modulo es consumido por APScheduler desde
app.py para ejecutar las tareas de forma automatica en segundo plano.

Tareas disponibles:
    - traer_partidos_del_dia: corre diariamente a las 6:00 AM.
    - sincronizar_resultados: corre cada 15 minutos en horario activo.
    - calcular_puntos_partido: invocada internamente al finalizar un partido.
    - cargar_proximos_dias: utilidad manual para carga inicial de fixtures.
    - cerrar_partidos_vencidos: corre diariamente a las 6:00 AM junto con
      traer_partidos_del_dia para sanear registros sin cerrar.
"""

import requests
import os
from dotenv import load_dotenv
from db.database import get_db_connection
from datetime import datetime, timedelta

load_dotenv()

API_KEY  = os.getenv('API_FOOTBALL_KEY')
HEADERS  = {'x-apisports-key': API_KEY}
BASE_URL = "https://v3.football.api-sports.io"

# Ligas de selecciones nacionales monitoreadas por el sistema.
# La clave es el ID de liga en la API de football.
LIGAS = {
    1:  "FIFA World Cup",
    4:  "Euro Championship",
    6:  "Africa Cup of Nations",
    9:  "Copa America",
    10: "Amistosos Internacionales",
    29: "Eliminatorias UEFA",
    30: "Eliminatorias CONMEBOL",
    31: "Eliminatorias CONCACAF",
    32: "Eliminatorias AFC",
    33: "Eliminatorias CAF",
}


def traer_partidos_del_dia():
    """
    Consulta la API y persiste los partidos programados para la fecha actual.

    Realiza una unica llamada al endpoint /fixtures filtrando por la fecha
    de hoy. Los partidos que ya existen en la base de datos son ignorados
    por la funcion auxiliar _guardar_partidos mediante una condicion
    IF NOT EXISTS en la insercion.

    Programada para correr diariamente a las 6:00 AM via APScheduler.
    """
    hoy = datetime.now().strftime('%Y-%m-%d')
    print(f"[{datetime.now().strftime('%H:%M')}] Trayendo partidos de {hoy}...")

    try:
        response = requests.get(
            f"{BASE_URL}/fixtures",
            headers=HEADERS,
            params={'date': hoy},
            timeout=10
        )
        data = response.json()

        if data.get('errors'):
            print(f"  Error API: {data['errors']}")
            return

        partidos = data.get('response', [])
        if not partidos:
            print("  Sin partidos hoy.")
            return

        nuevos = _guardar_partidos(partidos)
        print(f"  {len(partidos)} encontrados, {nuevos} nuevos guardados.")

    except Exception as e:
        print(f"  Error: {e}")


def sincronizar_resultados():
    """
    Actualiza el marcador y el estado de los partidos pendientes del dia.

    Ejecuta dos consultas a la base de datos: primero verifica si existen
    partidos sin finalizar para hoy; si los hay, realiza una sola llamada
    a la API con la fecha actual (compatible con el plan gratuito) y cruza
    los resultados contra los registros locales por api_fixture_id.

    Para cada partido activo o finalizado actualiza goles_local,
    goles_visitante y el campo finalizado. Si el partido termino
    (estados FT, PEN o AET), invoca calcular_puntos_partido para
    distribuir los puntos entre los usuarios que pronosticaron.

    Solo se ejecuta entre las 10:00 y las 23:00 horas para evitar
    llamadas innecesarias a la API durante la madrugada.

    Programada para correr cada 15 minutos via APScheduler.
    """
    ahora = datetime.now()
    hora  = ahora.hour

    if hora < 10 or hora > 23:
        print(f"  Sin partidos a las {hora}h. Pausado.")
        return

    hoy = ahora.strftime('%Y-%m-%d')
    print(f"[{ahora.strftime('%H:%M')}] Actualizando resultados...")

    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*)
            FROM wc_partidos
            WHERE finalizado = 0
              AND CAST(fecha_hora AS DATE) = CAST(GETDATE() AS DATE)
        """)
        pendientes = cursor.fetchone()[0]
        conn.close()

        if pendientes == 0:
            print("  Sin partidos pendientes hoy.")
            return

        print(f"  {pendientes} partidos pendientes. Consultando API...")

        response = requests.get(
            f"{BASE_URL}/fixtures",
            headers=HEADERS,
            params={'date': hoy},
            timeout=10
        )
        data = response.json()

        if data.get('errors'):
            print(f"  Error API: {data['errors']}")
            return

        partidos_api = data.get('response', [])
        if not partidos_api:
            print("  API no devolvio partidos.")
            return

        # Indexar la respuesta de la API por fixture ID para cruce eficiente O(1)
        api_dict = {}
        for item in partidos_api:
            f_id = item['fixture']['id']
            api_dict[f_id] = {
                'estado' : item['fixture']['status']['short'],
                'goles_l': item['goals']['home'],
                'goles_v': item['goals']['away'],
            }

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, api_fixture_id
            FROM wc_partidos
            WHERE finalizado = 0
              AND CAST(fecha_hora AS DATE) = CAST(GETDATE() AS DATE)
        """)
        pendientes_bd = cursor.fetchall()
        actualizados  = 0

        for row in pendientes_bd:
            bd_id = row[0]
            f_id  = row[1]

            if f_id not in api_dict:
                # La API no incluyo este fixture en la respuesta de hoy
                continue

            info       = api_dict[f_id]
            estado     = info['estado']
            goles_l    = info['goles_l']
            goles_v    = info['goles_v']
            # Los estados FT (tiempo reglamentario), PEN (penales) y AET
            # (tiempo extra) indican que el partido ha concluido definitivamente.
            finalizado = 1 if estado in ('FT', 'PEN', 'AET') else 0

            if goles_l is not None:
                cursor.execute("""
                    UPDATE wc_partidos
                    SET goles_local     = ?,
                        goles_visitante = ?,
                        finalizado      = ?
                    WHERE id = ?
                """, (goles_l, goles_v, finalizado, bd_id))

                if finalizado:
                    calcular_puntos_partido(f_id, goles_l, goles_v, cursor)
                    actualizados += 1

        conn.commit()
        conn.close()
        print(f"  {actualizados} partidos finalizados y puntos calculados.")

    except Exception as e:
        print(f"  Error: {e}")


def calcular_puntos_partido(api_fixture_id, real_l, real_v, cursor):
    """
    Evalua y asigna los puntos correspondientes a cada pronostico de un partido.

    Consulta todos los pronosticos pendientes de calificacion (puntos_obtenidos
    IS NULL) para el partido identificado por api_fixture_id y aplica la
    siguiente escala de puntuacion:

        5 puntos: marcador exacto (ambos goles coinciden).
        3 puntos: resultado correcto (ganador o empate acertado).
        2 puntos: al menos un equipo con goles exactos.
        0 puntos: ningun criterio cumplido.

    Tras asignar los puntos al pronostico individual, actualiza o crea el
    registro del usuario en la tabla wc_ranking mediante un MERGE de T-SQL.

    Args:
        api_fixture_id (int): Identificador del fixture en la API externa.
        real_l (int): Goles reales del equipo local al finalizar el partido.
        real_v (int): Goles reales del equipo visitante al finalizar el partido.
        cursor: Cursor de base de datos activo. La confirmacion (commit)
                es responsabilidad del llamador.
    """
    cursor.execute("""
        SELECT pr.id, pr.pronostico_local, pr.pronostico_visitante, pr.usuario_id
        FROM wc_pronosticos pr
        JOIN wc_partidos p ON pr.partido_id = p.id
        WHERE p.api_fixture_id = ?
          AND pr.puntos_obtenidos IS NULL
    """, (api_fixture_id,))

    pronosticos = cursor.fetchall()

    for pro in pronosticos:
        id_pro, p_l, p_v, u_id = pro[0], pro[1], pro[2], pro[3]
        puntos = 0

        if p_l == real_l and p_v == real_v:
            puntos = 5
        elif (
            (p_l > p_v and real_l > real_v) or
            (p_l < p_v and real_l < real_v) or
            (p_l == p_v and real_l == real_v)
        ):
            puntos = 3
        elif p_l == real_l or p_v == real_v:
            puntos = 2

        cursor.execute("""
            UPDATE wc_pronosticos
            SET puntos_obtenidos = ?
            WHERE id = ?
        """, (puntos, id_pro))

        # Actualiza el acumulado del usuario en wc_ranking, creando el registro
        # si todavia no existe (primer pronostico calificado del usuario).
        cursor.execute("""
            MERGE INTO wc_ranking AS target
            USING (SELECT ? AS uid) AS source
            ON (target.usuario_id = source.uid)
            WHEN MATCHED THEN
                UPDATE SET
                    puntos_total     = puntos_total + ?,
                    aciertos_exactos = aciertos_exactos + (CASE WHEN ? = 5 THEN 1 ELSE 0 END),
                    aciertos_ganador = aciertos_ganador + (CASE WHEN ? = 3 THEN 1 ELSE 0 END),
                    aciertos_goles   = aciertos_goles   + (CASE WHEN ? = 2 THEN 1 ELSE 0 END),
                    actualizado_en   = GETDATE()
            WHEN NOT MATCHED THEN
                INSERT (usuario_id, puntos_total, aciertos_exactos,
                        aciertos_ganador, aciertos_goles)
                VALUES (?, ?,
                    CASE WHEN ? = 5 THEN 1 ELSE 0 END,
                    CASE WHEN ? = 3 THEN 1 ELSE 0 END,
                    CASE WHEN ? = 2 THEN 1 ELSE 0 END);
        """, (u_id, puntos, puntos, puntos, puntos,
              u_id, puntos, puntos, puntos, puntos))

    if pronosticos:
        print(f"  Puntos calculados: {len(pronosticos)} pronosticos.")


def cargar_proximos_dias(dias=7):
    """
    Carga en la base de datos los fixtures de los proximos N dias para todas
    las ligas configuradas en el diccionario LIGAS.

    Itera cada combinacion liga x fecha y realiza una llamada independiente
    a la API por cada par. Los partidos ya existentes no se duplican gracias
    a la condicion IF NOT EXISTS en _guardar_partidos.

    Esta funcion es de uso manual y debe ejecutarse desde una consola o
    script de inicializacion antes de poner el sistema en produccion, o
    cuando se necesite rellenar dias futuros sin esperar al job diario.

    Args:
        dias (int): Cantidad de dias hacia adelante a cargar, incluyendo hoy.
                    Por defecto 7.
    """
    print(f"Cargando partidos de los proximos {dias} dias...")
    hoy          = datetime.now()
    nuevos_total = 0

    for league_id, nombre in LIGAS.items():
        for delta in range(dias):
            fecha = (hoy + timedelta(days=delta)).strftime('%Y-%m-%d')
            try:
                response = requests.get(
                    f"{BASE_URL}/fixtures",
                    headers=HEADERS,
                    params={'league': league_id, 'season': 2026, 'date': fecha},
                    timeout=10
                )
                data = response.json()
                if data.get('errors'):
                    continue
                partidos = data.get('response', [])
                if partidos:
                    nuevos = _guardar_partidos(partidos)
                    if nuevos > 0:
                        print(f"  {nombre} ({fecha}): {nuevos} nuevos")
                    nuevos_total += nuevos
            except Exception as e:
                print(f"  {nombre} {fecha}: {e}")

    print(f"\nTotal: {nuevos_total} partidos cargados.")


def _guardar_partidos(partidos):
    """
    Persiste una lista de fixtures de la API en la tabla wc_partidos.

    Recorre la lista de objetos retornados por el endpoint /fixtures y
    ejecuta un INSERT condicional por cada uno. Si el api_fixture_id ya
    existe en la tabla, la sentencia IF NOT EXISTS evita la duplicacion
    sin lanzar una excepcion.

    La fecha del fixture se normaliza eliminando la designacion de zona
    horaria (sufijo +HH:MM) y reemplazando el separador ISO 'T' por un
    espacio para compatibilidad con el tipo DATETIME de SQL Server.

    Args:
        partidos (list): Lista de objetos fixture tal como los retorna
                         la API de football en data['response'].

    Returns:
        int: Cantidad de registros nuevos insertados.
    """
    conn   = get_db_connection()
    cursor = conn.cursor()
    nuevos = 0

    for item in partidos:
        f_id   = item['fixture']['id']
        local  = item['teams']['home']['name']
        visita = item['teams']['away']['name']
        fecha  = item['fixture']['date'].replace('T', ' ').split('+')[0]
        fase   = item['league'].get('round', item['league']['name'])

        cursor.execute("""
            IF NOT EXISTS (SELECT 1 FROM wc_partidos WHERE api_fixture_id = ?)
            INSERT INTO wc_partidos
                (api_fixture_id, equipo_local, equipo_visitante, fecha_hora, fase, finalizado)
            VALUES (?, ?, ?, ?, ?, 0)
        """, (f_id, f_id, local, visita, fecha, fase))

        if cursor.rowcount > 0:
            nuevos += 1

    conn.commit()
    conn.close()
    return nuevos


def cerrar_partidos_vencidos():
    """
    Cierra partidos de dias anteriores que quedaron en estado pendiente.

    Esto ocurre cuando el scheduler no pudo procesar un partido en su dia,
    ya sea por agotamiento del limite de requests del plan gratuito de la API
    o por una caida del servicio. Para cada partido vencido sin finalizar,
    consulta la API individualmente por fixture ID y actualiza el marcador.

    Si la API retorna un error o no incluye el fixture, el partido se marca
    como finalizado de todas formas para evitar que quede bloqueando el sistema.
    Del mismo modo, si la llamada HTTP lanza una excepcion, el partido se cierra
    de forma defensiva.

    Programada para correr una vez al dia a las 6:00 AM junto con
    traer_partidos_del_dia via APScheduler.
    """
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT id, api_fixture_id, equipo_local, equipo_visitante
            FROM wc_partidos
            WHERE finalizado = 0
              AND CAST(fecha_hora AS DATE) < CAST(GETDATE() AS DATE)
        """)
        vencidos = cursor.fetchall()
        conn.close()

        if not vencidos:
            return

        print(f"  {len(vencidos)} partidos vencidos sin cerrar. Consultando API...")

        conn     = get_db_connection()
        cursor   = conn.cursor()
        cerrados = 0

        for row in vencidos:
            bd_id = row[0]
            f_id  = row[1]

            try:
                response = requests.get(
                    f"{BASE_URL}/fixtures",
                    headers=HEADERS,
                    params={'id': f_id},
                    timeout=10
                )
                data = response.json()

                if data.get('errors'):
                    # Sin acceso al fixture: cierre defensivo para no bloquear el sistema
                    cursor.execute(
                        "UPDATE wc_partidos SET finalizado = 1 WHERE id = ?", (bd_id,)
                    )
                    cerrados += 1
                    continue

                items = data.get('response', [])
                if not items:
                    cursor.execute(
                        "UPDATE wc_partidos SET finalizado = 1 WHERE id = ?", (bd_id,)
                    )
                    cerrados += 1
                    continue

                item    = items[0]
                estado  = item['fixture']['status']['short']
                goles_l = item['goals']['home']
                goles_v = item['goals']['away']

                cursor.execute("""
                    UPDATE wc_partidos
                    SET goles_local     = ?,
                        goles_visitante = ?,
                        finalizado      = 1
                    WHERE id = ?
                """, (goles_l, goles_v, bd_id))

                if goles_l is not None and goles_v is not None:
                    calcular_puntos_partido(f_id, goles_l, goles_v, cursor)

                cerrados += 1

            except Exception:
                # Cierre defensivo ante cualquier fallo de red o parseo
                cursor.execute(
                    "UPDATE wc_partidos SET finalizado = 1 WHERE id = ?", (bd_id,)
                )
                cerrados += 1

        conn.commit()
        conn.close()
        print(f"  {cerrados} partidos vencidos cerrados.")

    except Exception as e:
        print(f"  Error en cerrar_partidos_vencidos: {e}")