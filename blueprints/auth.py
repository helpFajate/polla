"""
Blueprint de autenticacion.

Gestiona el inicio de sesion, registro y cierre de sesion de usuarios.
Utiliza Flask-Login para el manejo de sesiones y Flask-Bcrypt para
el cifrado seguro de contrasenas.
"""

from flask import Blueprint, render_template, redirect, url_for, request, flash
from flask_login import login_user, logout_user, login_required
from flask_bcrypt import Bcrypt
from db.database import get_db_connection

auth_bp = Blueprint('auth', __name__)
bcrypt = Bcrypt()


@auth_bp.route('/login', methods=['GET', 'POST'])
def login():
    """
    Maneja el inicio de sesion del usuario.

    GET: Renderiza el formulario de inicio de sesion.
    POST: Valida las credenciales contra la base de datos. Si son correctas,
          crea la sesion del usuario mediante una cookie en el navegador
          y redirige al listado de partidos. En caso contrario, muestra
          un mensaje de error.
    """
    if request.method == 'POST':
        usuario_input = request.form.get('usuario')
        password_input = request.form.get('password')

        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute(
            "SELECT id, nombre, usuario, password FROM wc_usuarios WHERE usuario = ?",
            (usuario_input,)
        )
        user_row = cursor.fetchone()
        conn.close()

        if user_row and bcrypt.check_password_hash(user_row[3], password_input):
            # Importacion local para evitar dependencia circular con app.py
            from app import User
            usuario_obj = User(id=user_row[0], nombre=user_row[1], usuario=user_row[2])
            login_user(usuario_obj)
            flash('Bienvenido al Pronosticador!', 'success')
            return redirect(url_for('partidos.listar_partidos'))
        else:
            flash('Usuario o contrasena incorrectos', 'danger')

    return render_template('login.html')


@auth_bp.route('/registro', methods=['GET', 'POST'])
def registro():
    """
    Maneja el registro de nuevos usuarios.

    GET: Renderiza el formulario de registro.
    POST: Recibe los datos del formulario, hashea la contrasena antes de
          almacenarla y persiste el nuevo usuario en la base de datos.
          Si el nombre de usuario ya existe, informa al cliente mediante
          un mensaje flash.
    """
    if request.method == 'POST':
        nombre = request.form.get('nombre')
        usuario = request.form.get('usuario')
        password_hashed = bcrypt.generate_password_hash(
            request.form.get('password')
        ).decode('utf-8')

        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            cursor.execute(
                """
                INSERT INTO wc_usuarios (nombre, usuario, password)
                VALUES (?, ?, ?)
                """,
                (nombre, usuario, password_hashed)
            )
            conn.commit()
            conn.close()
            flash('Registro exitoso. Ya puedes iniciar sesion!', 'success')
            return redirect(url_for('auth.login'))
        except Exception as e:
            print(f"Error en registro: {e}")
            flash('El usuario ya existe o hubo un error en la base de datos.', 'danger')

    return render_template('registro.html')


@auth_bp.route('/logout')
@login_required
def logout():
    """
    Cierra la sesion del usuario autenticado.

    Invalida la sesion activa y redirige al formulario de inicio de sesion.
    Requiere que el usuario este autenticado para acceder a esta ruta.
    """
    logout_user()
    flash('Has cerrado sesion correctamente.', 'info')
    return redirect(url_for('auth.login'))