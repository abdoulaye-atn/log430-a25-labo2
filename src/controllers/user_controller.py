"""
User controller
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from commands.write_user import add_user, delete_user_by_id
from queries.read_user import get_users

def create_user(name, email):
    """Create user, use WriteUser model"""
    try:
        return add_user(name, email)
    except Exception as e:
        print(e)
        return "Une erreur s'est produite lors de la création de l'enregistrement. Veuillez consulter les logs pour plus d'informations."

def delete_user(user_id):
    """Delete user, use WriteUser model"""
    try:
        return delete_user_by_id(user_id)
    except Exception as e:
        print(e)
        return "Une erreur s'est produite lors de la supression de l'enregistrement. Veuillez consulter les logs pour plus d'informations."

def list_users(limit):
    """Get last X users, use ReadUser model"""
    try:
        return get_users(limit)
    except Exception as e:
        print(e)
        return "Une erreur s'est produite lors de la requête de base de données. Veuillez consulter les logs pour plus d'informations."