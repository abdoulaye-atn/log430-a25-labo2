"""
Orders (write-only model)
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
import json
from models.product import Product
from models.order_item import OrderItem
from models.order import Order
from queries.read_order import get_orders_from_mysql
from db import get_sqlalchemy_session, get_redis_conn

def add_order(user_id: int, items: list):
    """Insert order with items in MySQL, keep Redis in sync"""
    if not user_id or not items:
        raise ValueError("Vous devez indiquer au moins 1 utilisateur et 1 item pour chaque commande.")

    try:
        product_ids = []
        for item in items:
            product_ids.append(int(item['product_id']))
    except Exception as e:
        print(e)
        raise ValueError(f"L'ID Article n'est pas valide: {item['product_id']}")
    session = get_sqlalchemy_session()

    try:
        products_query = session.query(Product).filter(Product.id.in_(product_ids)).all()
        price_map = {product.id: product.price for product in products_query}
        total_amount = 0
        order_items_data = []
        
        for item in items:
            pid = int(item["product_id"])
            qty = float(item["quantity"])

            if not qty or qty <= 0:
                raise ValueError(f"Vous devez indiquer une quantité superieure à zéro.")

            if pid not in price_map:
                raise ValueError(f"Article ID {pid} n'est pas dans la base de données.")

            unit_price = price_map[pid]
            total_amount += unit_price * qty
            order_items_data.append({
                'product_id': pid,
                'quantity': qty,
                'unit_price': unit_price
            })
        
        new_order = Order(user_id=user_id, total_amount=total_amount)
        session.add(new_order)
        session.flush() 
        
        order_id = new_order.id

        for item_data in order_items_data:
            order_item = OrderItem(
                order_id=order_id,
                product_id=item_data['product_id'],
                quantity=item_data['quantity'],
                unit_price=item_data['unit_price']
            )
            session.add(order_item)

        session.commit()

        # TODO: ajouter la commande à Redis
        add_order_to_redis(order_id, user_id, total_amount, items)

        return order_id

    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

def delete_order(order_id: int):
    """Delete order in MySQL, keep Redis in sync"""
    session = get_sqlalchemy_session()
    try:
        order = session.query(Order).filter(Order.id == order_id).first()
        
        if order:
            session.delete(order)
            session.commit()

            # TODO: supprimer la commande à Redis
            delete_order_from_redis(order_id)
            return 1  
        else:
            return 0  
            
    except Exception as e:
        session.rollback()
        raise e
    finally:
        session.close()

#QUESTION 3
def add_order_to_redis(order_id, user_id, total_amount, items):
    """Insert order to Redis"""
    r = get_redis_conn()
    key = f"order:{order_id}"

    # (optionnel) si la clé existe déjà, on peut sortir pour rester idempotent
    # if r.exists(key):
    #     return 0

    pipe = r.pipeline()
    # 1) hash de la commande
    pipe.hset(key, mapping={
        "id": int(order_id),
        "user_id": int(user_id),
        "total_amount": float(total_amount),
        "items": json.dumps(items, ensure_ascii=False)  # pratique pour l’affichage/rapports
    })
    # 2) index trié des commandes (score = id → récup des N dernières via ZREVRANGE)
    pipe.zadd("orders:index", {key: int(order_id)})
    
    #############################################
    ################## QUESTION 6 ################
    # incrémenter les compteurs par produit
    for it in items:
        pid = int(it["product_id"])
        qty = int(float(it["quantity"]))
        pipe.incrby(f"product:{pid}:sold_qty", qty)
    #############################################

    pipe.execute()
    return 1


#QUESTION 4
def delete_order_from_redis(order_id):
    """Delete order from Redis"""
    r = get_redis_conn()
    key = f"order:{order_id}"

    pipe = r.pipeline()
    # 1) retirer la clé de l'index trié
    pipe.zrem("orders:index", key)
    # 2) supprimer le hash de la commande
    pipe.delete(key)
    pipe.execute()
    return 1  # idempotent : même si la clé n'existait pas, on considère OK


def sync_all_orders_to_redis():
    """ Sync orders from MySQL to Redis """
    # redis
    r = get_redis_conn()
    orders_in_redis = r.keys(f"order:*")
    rows_added = 0
    try:
        if len(orders_in_redis) == 0:
            # mysql
            orders_from_mysql = get_orders_from_mysql(limit=9999) or []
            pipe = r.pipeline()
            for order in orders_from_mysql:
                key = f"order:{order.id}"
                pipe.hset(key, mapping={
                    "id": int(order.id),
                    "user_id": int(order.user_id),
                    "total_amount": float(order.total_amount)
                })
                pipe.zadd("orders:index", {key: int(order.id)})
            if orders_from_mysql:
                pipe.execute()
            rows_added = len(orders_from_mysql)
        else:
            print('Redis already contains orders, no need to sync!')
    except Exception as e:
        print(e)
        return 0
    finally:
        return len(orders_in_redis) + rows_added