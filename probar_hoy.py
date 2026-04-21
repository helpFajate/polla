import requests
import os
from dotenv import load_dotenv
from db.database import get_db_connection
from datetime import datetime, timedelta

load_dotenv()

API_KEY  = os.getenv('API_FOOTBALL_KEY')
HEADERS  = {'x-apisports-key': API_KEY}
BASE_URL = "https://v3.football.api-sports.io"

# Sin filtrar por liga ni season — trae TODO lo que haya
def traer_partidos_rango(dias_adelante=7):
    hoy = datetime.now()
    nuevos_total = 0

    print(f"📡 Buscando partidos de los próximos {dias_adelante} días...\n")

    for delta in range(dias_adelante):
        fecha = (hoy + timedelta(days=delta)).strftime('%Y-%m-%d')
        try:
            response = requests.get(
                f"{BASE_URL}/fixtures",
                headers=HEADERS,
                params={'date': fecha},
                timeout=10
            )
            data = response.json()

            if data.get('errors'):
                print(f"  ❌ Error API: {data['errors']}")
                continue

            partidos = data.get('response', [])
            if not partidos:
                print(f"  ℹ️ {fecha}: sin partidos")
                continue

            nuevos = guardar_partidos(partidos)
            print(f"  ✅ {fecha}: {len(partidos)} encontrados, {nuevos} nuevos guardados")
            nuevos_total += nuevos

        except Exception as e:
            print(f"  ❌ {fecha}: {e}")

    print(f"\n🚀 Total: {nuevos_total} partidos nuevos cargados.")


def guardar_partidos(partidos):
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
                (api_fixture_id, equipo_local, equipo_visitante,
                 fecha_hora, fase, finalizado)
            VALUES (?, ?, ?, ?, ?, 0)
        """, (f_id, f_id, local, visita, fecha, fase))

        if cursor.rowcount > 0:
            nuevos += 1

    conn.commit()
    conn.close()
    return nuevos


if __name__ == "__main__":
    traer_partidos_rango(dias_adelante=7)