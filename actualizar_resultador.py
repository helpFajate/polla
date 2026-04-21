import requests
from db.database import get_db_connection

API_KEY = "TU_API_KEY_AQUI"
HEADERS = {
    'x-rapidapi-key': API_KEY,
    'x-rapidapi-host': "api-football-v1.p.rapidapi.com"
}

def actualizar_marcadores_reales():
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Buscamos los IDs de partidos que no han terminado en nuestra DB
    cursor.execute("SELECT api_fixture_id FROM wc_partidos WHERE finalizado = 0")
    partidos_pendientes = cursor.fetchall()

    for p in partidos_pendientes:
        f_id = p[0]
        url = f"https://api-football-v1.p.rapidapi.com/v3/fixtures?id={f_id}"
        response = requests.get(url, headers=HEADERS).json()

        if response['response']:
            item = response['response'][0]
            status = item['fixture']['status']['short']
            
            # Si el partido ya terminó (FT = Full Time)
            if status in ['FT', 'AET', 'PEN']:
                goles_l = item['goals']['home']
                goles_v = item['goals']['away']
                
                cursor.execute("""
                    UPDATE wc_partidos 
                    SET goles_local = ?, goles_visitante = ?, finalizado = 1 
                    WHERE api_fixture_id = ?
                """, (goles_l, goles_v, f_id))
    
    conn.commit()
    conn.close()
    print("✅ Marcadores reales actualizados.")

if __name__ == "__main__":
    actualizar_marcadores_reales()