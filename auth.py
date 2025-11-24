""""Genera el hash de una contraseña utilizando SHA-256.

@param password Contraseña en texto plano a convertir en hash.

@returns Cadena con el hash resultante de la contraseña.
"""
import json
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional, Union
import streamlit as st

USERS_PATH: Path = Path('users.json')


def _hash_password(password: str) -> str:
    return hashlib.sha256(password.encode('utf-8')).hexdigest()


""""Carga los usuarios desde el archivo JSON, o crea uno por defecto si no existe.

@returns Diccionario con los usuarios y sus datos (hash de contraseña y permisos).
"""
def load_users() -> Dict[str, Any]:
    if not USERS_PATH.exists():
        default = {'admin': {'password_hash': _hash_password('admin123'), 'is_admin': True}}
        save_users(default)
        return default
    try:
        with USERS_PATH.open('r', encoding='utf-8') as f:
            return json.load(f)
    except Exception:
        return {}


""""Guarda los usuarios en el archivo JSON.

@param users Diccionario que contiene los datos de los usuarios a guardar.

@returns None. Escribe directamente en el archivo.
"""
def save_users(users: Dict[str, Any]) -> None:
    with USERS_PATH.open('w', encoding='utf-8') as f:
        json.dump(users, f, indent=2)


""""Crea un nuevo usuario con nombre, contraseña y rol (admin o no).

@param username Nombre de usuario.
@param password Contraseña del usuario.
@param is_admin Indica si el usuario es administrador (por defecto False).

@returns True si el usuario fue creado exitosamente, False si ya existe.
"""
def create_user(username: str, password: str, is_admin: bool = False) -> bool:
    users = load_users()
    if username in users:
        return False
    users[username] = {'password_hash': _hash_password(password), 'is_admin': bool(is_admin)}
    save_users(users)
    return True


""""Verifica las credenciales de un usuario.

@param username Nombre de usuario.
@param password Contraseña en texto plano a verificar.

@returns True si las credenciales son correctas, False en caso contrario.
"""
def verify_user(username: str, password: str) -> bool:
    users = load_users()
    if username not in users:
        return False
    return users[username].get('password_hash') == _hash_password(password)


""""Verifica si un usuario tiene privilegios de administrador.

@param username Nombre del usuario.

@returns True si el usuario es administrador, False en caso contrario.
"""
def is_admin(username: str) -> bool:
    users = load_users()
    return bool(users.get(username, {}).get('is_admin', False))


""""Muestra la interfaz de inicio de sesión para administradores en la barra lateral.

@returns True si hay una sesión activa de administrador, False en caso contrario.
"""
def admin_login_ui() -> bool:
    if 'is_admin' not in st.session_state:
        st.session_state['is_admin'] = False
    if st.session_state.get('is_admin'):
        return True

    with st.sidebar:
        st.subheader("Acceso Administrador")
        username = st.text_input("Usuario", key="admin")
        password = st.text_input("Contraseña", type="password", key="admin123")
        if st.button("Iniciar Sesión", key="admin_login_btn"):
            if verify_user(username, password) and is_admin(username):
                st.session_state['is_admin'] = True
                st.session_state['_admin_user'] = username
                st.success(f'Ingresado como administrador: {username}')
                return True
            else:
                st.error("Credenciales incorrectas o usuario no es admin")
    return False


""""Cierra la sesión del administrador actual.

@returns None. Limpia las variables de sesión relacionadas al administrador.
"""
def admin_logout() -> None:
    if 'is_admin' in st.session_state:
        st.session_state['is_admin'] = False
    if '_admin_user' in st.session_state:
        st.session_state['_admin_user'] = None


""""Obtiene el nombre del administrador actualmente autenticado.

@returns Nombre del administrador en sesión o None si no hay sesión activa.
"""
def current_admin() -> Optional[str]:
    return st.session_state.get('_admin_user')
