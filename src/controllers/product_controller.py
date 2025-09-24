"""
Product controller
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
from commands.write_product import add_product, delete_product_by_id
from queries.read_product import get_products

def create_product(name, sku, price):
    """Create product, use WriteProduct model"""
    try:
        return add_product(name, sku, price)
    except ValueError as e:
        return str(e)
    except Exception as e:
        print(e)
        return "Une erreur s'est produite lors de la création de l'enregistrement. Veuillez consulter les logs pour plus d'informations."

def delete_product(product_id):
    """Delete product, use WriteProduct model"""
    try:
        return delete_product_by_id(product_id)
    except Exception as e:
        print(e)
        return "Une erreur s'est produite lors de la supression de l'enregistrement. Veuillez consulter les logs pour plus d'informations."

def list_products(limit):
    """Get last X products, use ReadProduct model"""
    try:
        return get_products(limit)
    except Exception as e:
        print(e)
        return "Une erreur s'est produite lors de la requête de base de données. Veuillez consulter les logs pour plus d'informations."