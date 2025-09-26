"""
Orders (read-only model)
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

from collections import defaultdict
import json
from db import get_sqlalchemy_session, get_redis_conn
from sqlalchemy import desc
from models.order import Order

#QUESTION 2 EN PARTIE
def get_order_by_id(order_id):
    """Get order by ID from Redis"""
    r = get_redis_conn()
    key = f"order:{order_id}"            
    raw = r.hgetall(key)
    if not raw:
        return {}
    return {
        "id": int(raw.get(b"id", b"0")),
        "user_id": int(float(raw.get(b"user_id", b"0"))),
        "total_amount": float(raw.get(b"total_amount", b"0")),
        "items": json.loads(raw.get(b"items", b"[]").decode() or "[]")
    }

def get_orders_from_mysql(limit=9999):
    """Get last X orders"""
    session = get_sqlalchemy_session()
    return session.query(Order).order_by(desc(Order.id)).limit(limit).all()


# QUESTION 2
def get_orders_from_redis(limit=9999):
    """Get last X orders"""
    r = get_redis_conn()
    keys = r.zrevrange("orders:index", 0, max(0, limit-1))
    results = []
    for k in keys:
        raw = r.hgetall(k)
        if not raw:
            continue
        results.append({
            "id": int(raw.get(b"id", b"0")),
            "user_id": int(float(raw.get(b"user_id", b"0"))),
            "total_amount": float(raw.get(b"total_amount", b"0")),
            "items": json.loads(raw.get(b"items", b"[]").decode() or "[]")
        })
    return results


#QUESTION 5
def get_highest_spending_users(limit=10):
    """Get report of best selling products"""
    # TODO: écrivez la méthode
    r = get_redis_conn()
    keys = r.zrevrange("orders:index", 0, -1)

    def to_text(v):
        if isinstance(v, (bytes, bytearray)):
            return v.decode()
        return str(v)

    def hget2(d, key):
        if key in d:
            return d[key]
        bkey = key.encode() if isinstance(key, str) else key
        skey = key.decode() if isinstance(key, (bytes, bytearray)) else key
        if bkey in d:
            return d[bkey]
        if skey in d:
            return d[skey]
        return None

    expenses_by_user = defaultdict(float)
    for k in keys:
        raw = r.hgetall(k)
        if not raw:
            continue

        uid_raw = hget2(raw, "user_id")
        tot_raw = hget2(raw, "total_amount")
        if uid_raw is None or tot_raw is None:
            continue

        try:
            user_id = int(float(to_text(uid_raw)))
            total   = float(to_text(tot_raw))
        except Exception:
            continue

        expenses_by_user[user_id] += total

    highest = sorted(expenses_by_user.items(), key=lambda it: it[1], reverse=True)
    return highest[:limit]

#QUESTION 6
def get_best_selling_products(limit=10):
    """Top produits par quantité vendue (Redis)"""
    r = get_redis_conn()

    keys = []
    cursor = 0
    pattern = "product:*:sold_qty"

    while True:
        cursor, batch = r.scan(cursor=cursor, match=pattern, count=500)
        keys.extend(batch)
        if cursor == 0:
            break

    if not keys:
        return []

    pipe = r.pipeline()
    for k in keys:
        pipe.get(k)
    counts = pipe.execute()

    results = []
    for k, v in zip(keys, counts):
        if v is None:
            continue

        if isinstance(k, bytes):
            name = k.decode()
        else:
            name = k

        parts = name.split(":")
        if len(parts) >= 3:
            try:
                pid = int(parts[1])
            except ValueError:
                continue
        else:
            continue

        if isinstance(v, bytes):
            try:
                qty = int(v.decode())
            except Exception:
                continue
        else:
            try:
                qty = int(v)
            except Exception:
                continue

        results.append((pid, qty))

    results.sort(key=lambda t: t[1], reverse=True)
    return results[:limit]