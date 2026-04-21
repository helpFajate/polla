import requests
from db.database import get_db_connection

API_KEY = "c6a5b4a69344ef3bb7b4582a31c77780"
HEADERS = {'x-rapidapi-key': API_KEY, 'x-rapidapi-host': 'v3.football.api-sports.io'}

def sincronizar_partidos_del_dia():
    # Ejemplo: Traer partidos de la liga/copa específica (ID 1 es amistosos, 10 es Mundial, etc.)
    # Puedes filtrar por fecha actual
    url = "https://v3.football.api-sports.io/fixtures?date=2026-03-26" 
    
    response = requests.get(url, headers=HEADERS)
    data = response.json()

    conn = get_db_connection()
    cursor = conn.cursor()

    for item in data['response']:
        f_id = item['fixture']['id']
        local = item['teams']['home']['name']
        visita = item['teams']['away']['name']
        goles_l = item['goals']['home']
        goles_v = item['goals']['away']
        status = item['fixture']['status']['short'] # 'FT' significa finalizado
        fecha = item['fixture']['date']

        # Si el equipo es Colombia o los que te interesan, los guardamos/actualizamos
        cursor.execute("""
            MERGE INTO wc_partidos AS target
            USING (SELECT ? AS api_id) AS source
            ON (target.api_fixture_id = source.api_id)
            WHEN MATCHED THEN
                UPDATE SET goles_local = ?, goles_visitante = ?, 
                           finalizado = (CASE WHEN ? = 'FT' THEN 1 ELSE 0 END)
            WHEN NOT MATCHED THEN
                INSERT (api_fixture_id, equipo_local, equipo_visitante, fecha_hora, fase, finalizado)
                VALUES (?, ?, ?, ?, 'Jornada Diaria', 0);
        """, (f_id, goles_l, goles_v, status, f_id, local, visita, fecha))
    
    conn.commit()
    conn.close()
    print("✅ Sincronización con la API completada.")

if __name__ == "__main__":
    sincronizar_partidos_del_dia()