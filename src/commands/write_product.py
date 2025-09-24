"""
Products (write-only model)
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

from sqlalchemy import desc
from models.product import Product
from db import get_sqlalchemy_session

def add_product(name: str, sku: str, price: float):
    """Insert product with items in MySQL"""
    if not name or not sku or not price or float(price) <= 0:
        raise ValueError("Vous devez indiquer un nom, numéro SKU et prix unitaire pour l'article.")
    
    session = get_sqlalchemy_session()

    try: 
        new_product = Product(name=name, sku=sku, price=price)
        session.add(new_product)
        session.flush() 
        session.commit()
        return new_product.id
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()


def delete_product_by_id(product_id: int):
    """Delete product by ID in MySQL"""
    session = get_sqlalchemy_session()
    try:
        product = session.query(Product).filter(Product.id == product_id).first()
        
        if product:
            session.delete(product)
            session.commit()
            return 1  
        else:
            return 0  
            
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

