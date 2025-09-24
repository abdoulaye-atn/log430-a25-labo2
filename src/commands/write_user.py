"""
Users (write-only model)
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

from sqlalchemy import desc
from models.user import User
from db import get_sqlalchemy_session

def add_user(name: str, email: str):
    """Insert user with items in MySQL"""
    if not name or not email:
        raise ValueError("Vous devez indiquer un nom et adresse courriel pour l'utilisateur.")
    
    session = get_sqlalchemy_session()

    try: 
        new_user = User(name=name, email=email)
        session.add(new_user)
        session.flush() 
        session.commit()
        return new_user.id
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def delete_user_by_id(user_id: int):
    """Delete user by ID in MySQL"""
    session = get_sqlalchemy_session()
    try:
        user = session.query(User).filter(User.id == user_id).first()
        if user:
            session.delete(user)
            session.commit()
            return 1  
        else:
            return 0  
            
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

