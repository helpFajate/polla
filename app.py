"""
Punto de entrada de la aplicacion Flask.

Inicializa la aplicacion mediante el patron Application Factory (create_app),
registra los blueprints, configura Flask-Login y arranca el scheduler de
tareas en segundo plano. El scheduler se detiene limpiamente al cerrar el
proceso gracias al hook de atexit.
"""

from flask import Flask
from flask_login import LoginManager, UserMixin
from config import Config
from db.database import query_db
from apscheduler.schedulers.background import BackgroundScheduler
from scheduler import sincronizar_resultados, traer_partidos_del_dia, cerrar_partidos_vencidos
import atexit


class User(UserMixin):
    """
    Modelo de usuario para Flask-Login.

    Extiende UserMixin para heredar las implementaciones por defecto de
    is_authenticated, is_active, is_anonymous y get_id, requeridas por
    la interfaz de Flask-Login.

    Attributes:
        id (int): Identificador primario del usuario en wc_usuarios.
        nombre (str): Nombre completo del usuario.
        usuario (str): Nombre de usuario utilizado para iniciar sesion.
    """
    def __init__(self, id, nombre, usuario):
        self.id      = id
        self.nombre  = nombre
        self.usuario = usuario


def create_app():
    """
    Fabrica de aplicacion Flask.

    Construye y configura la instancia de Flask siguiendo el patron
    Application Factory, lo que facilita la creacion de instancias
    aisladas para pruebas y distintos entornos.

    Pasos de inicializacion:
        1. Carga la configuracion desde el objeto Config.
        2. Configura Flask-Login con la vista de login y el cargador
           de usuarios desde la base de datos.
        3. Registra los cuatro blueprints: auth, partidos, pronosticos
           y ranking.
        4. Inicia el BackgroundScheduler con dos jobs cron a las 6:00 AM
           (traer_partidos_del_dia + cerrar_partidos_vencidos) y un job
           de intervalo cada 15 minutos (sincronizar_resultados).

    Returns:
        Flask: Instancia de la aplicacion completamente configurada.
    """
    app = Flask(__name__)
    app.config.from_object(Config)

    # Configuracion de Flask-Login
    login_manager = LoginManager()
    login_manager.login_view    = 'auth.login'
    login_manager.login_message = "Por favor inicia sesion para acceder."
    login_manager.init_app(app)

    @login_manager.user_loader
    def load_user(user_id):
        """
        Recarga el objeto User desde la base de datos para cada request.

        Flask-Login invoca esta funcion en cada peticion autenticada
        pasando el user_id almacenado en la cookie de sesion.

        Args:
            user_id (str): ID del usuario serializado en la sesion.

        Returns:
            User | None: Instancia del usuario si existe, None en caso contrario.
        """
        res = query_db(
            "SELECT id, nombre, usuario FROM wc_usuarios WHERE id = ?",
            (user_id,),
            one=True
        )
        if res:
            return User(id=res['id'], nombre=res['nombre'], usuario=res['usuario'])
        return None

    # Registro de blueprints
    from blueprints.auth        import auth_bp
    from blueprints.partidos    import partidos_bp
    from blueprints.pronosticos import pronosticos_bp
    from blueprints.ranking     import ranking_bp

    app.register_blueprint(auth_bp)
    app.register_blueprint(partidos_bp)
    app.register_blueprint(pronosticos_bp)
    app.register_blueprint(ranking_bp)

    # Inicializacion del scheduler de tareas en segundo plano
    try:
        scheduler = BackgroundScheduler()

        # Trae los partidos del dia y cierra los vencidos: diariamente a las 6:00 AM.
        # Se combinan en un lambda para garantizar que ambas tareas compartan el
        # mismo slot de ejecucion y no colisionen con el job de sincronizacion.
        scheduler.add_job(
            func=lambda: [traer_partidos_del_dia(), cerrar_partidos_vencidos()],
            trigger='cron',
            hour=6,
            minute=0,
            id='traer_y_cerrar_partidos'
        )

        # Sincroniza resultados en vivo: cada 15 minutos.
        scheduler.add_job(
            func=sincronizar_resultados,
            trigger='interval',
            minutes=15,
            id='sync_resultados'
        )

        scheduler.start()
        # Garantiza el apagado limpio del scheduler al terminar el proceso.
        atexit.register(lambda: scheduler.shutdown())
        print("Scheduler iniciado correctamente.")

    except Exception as e:
        print(f"Error iniciando scheduler: {e}")

    return app


if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=5000, debug=True, use_reloader=False)