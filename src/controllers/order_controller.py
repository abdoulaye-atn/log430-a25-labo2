"""
Order controller
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from commands.write_order import add_order, delete_order, sync_all_orders_to_redis
from queries.read_order import get_best_selling_products, get_highest_spending_users, get_orders_from_mysql, get_orders_from_redis

def create_order(user_id, items):
    """Create order, use WriteOrder model"""
    try:
        return add_order(user_id, items)
    except ValueError as e:
        return str(e)
    except Exception as e:
        print(e)
        return "Une erreur s'est produite lors de la création de l'enregistrement. Veuillez consulter les logs pour plus d'informations."

def remove_order(order_id):
    """Delete order, use WriteOrder model"""
    try:
        return delete_order(order_id)
    except Exception as e:
        print(e)
        return "Une erreur s'est produite lors de la supression de l'enregistrement. Veuillez consulter les logs pour plus d'informations."

def list_orders_from_mysql(limit):
    """Get last X orders from MySQL, use ReadOrder model"""
    try:
        return get_orders_from_mysql(limit)
    except Exception as e:
        print(e)
        return "Une erreur s'est produite lors de la requête de base de données. Veuillez consulter les logs pour plus d'informations."
    
def list_orders_from_redis(limit):
    """Get last X orders from Redis, use ReadOrder model"""
    try:
        #QUESTION 2
        return get_orders_from_redis(limit)
    except Exception as e:
        print(e)
        return "Une erreur s'est produite lors de la requête de base de données. Veuillez consulter les logs pour plus d'informations."
    
def populate_redis_from_mysql():
   """Populate Redis with orders from MySQL, only if MySQL is empty"""
   sync_all_orders_to_redis()

#QUESTION 5
def get_report_highest_spending_users(limit=10):
    """Get orders report: highest spending users"""
    # TODO: appeler la méthode correspondante dans read_order.py
    return get_highest_spending_users(limit)

#QUESTION 6
def get_report_best_sellers(limit=10):
    """Rapport: articles les plus vendus (Redis)"""
    return get_best_selling_products(limit)