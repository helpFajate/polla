"""
Configuracion centralizada de la aplicacion.

Lee las variables de entorno desde el archivo .env mediante python-dotenv
y las expone como atributos de la clase Config, que es consumida por
Flask a traves de app.config.from_object(Config).

Variables de entorno requeridas:
    SECRET_KEY         Clave secreta para firmar las cookies de sesion de Flask.
    API_FOOTBALL_KEY   Clave de autenticacion para la API de football.

Variables de entorno opcionales (con valor por defecto):
    SQL_SERVER         Nombre o direccion del servidor SQL Server.
    SQL_DATABASE       Nombre de la base de datos. Por defecto 'Reportes'.
"""

import os
from dotenv import load_dotenv

load_dotenv()


class Config:
    SECRET_KEY        = os.getenv('SECRET_KEY', 'mundial-2026-secret')
    DB_SERVER         = os.getenv('SQL_SERVER')
    DB_NAME           = os.getenv('SQL_DATABASE', 'Reportes')
    API_FOOTBALL_KEY  = os.getenv('API_FOOTBALL_KEY')
    API_FOOTBALL_URL  = "https://v3.football.api-sports.io"