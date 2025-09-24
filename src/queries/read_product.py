"""
Product (read-only model)
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

from sqlalchemy import desc
from db import get_sqlalchemy_session
from models.product import Product

def get_product_by_id(product_id):
    """Get product by ID """
    session = get_sqlalchemy_session()
    result = session.query(Product).filter_by(id=product_id).all()

    if len(result):
        return {
            'id': result[0].id,
            'name': result[0].name,
            'sku': result[0].sku,
            'price': result[0].price
        }
    else:
        return {}

def get_products(limit=9999):
    """Get last X products"""
    session = get_sqlalchemy_session()
    return session.query(Product).order_by(desc(Product.id)).limit(limit).all()