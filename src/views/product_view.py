"""
Product view
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
import numbers
from views.template_view import get_template, get_param
from controllers.product_controller import create_product, delete_product, list_products

def show_product_form():
    """ Show product form and list """
    products = list_products(10)
    rows = [f"""
            <tr>
                <td>{product.id}</td>
                <td>{product.name}</td>
                <td>{product.sku}</td>
                <td>${product.price}</td>
                <td><a href="/products/remove/{product.id}">Supprimer</a></td>
            </tr> """ for product in products]
    return get_template(f"""
        <h2>Articles</h2>
        <p>Voici les 10 derniers enregistrements :</p>
        <table class="table">
            <tr>
                <th>ID</th> 
                <th>Nom</th>
                <th>Numéro SKU</th>
                <th>Prix unitaire</th> 
                <th>Actions</th> 
            </tr>  
            {" ".join(rows)}
        </table>
        <h2>Enregistrement</h2>
        <form method="POST" action="/products/add">
            <div class="mb-3">
                <label class="form-label">Nom</label>
                <input class="form-control" type="text" name="name" maxlength="100" required>
            </div>
            <div class="mb-3">
                <label class="form-label">Numéro SKU</label>
                <input class="form-control" type="text" name="sku" maxlength="64" required>
            </div>
            <div class="mb-3">
                <label class="form-label">Prix unitaire</label>
                <input class="form-control" type="number" name="price" step="0.01" value="1.00" min="0.00" max="99999.00" required>
            </div>
            <button type="submit" class="btn btn-primary">Enregistrer</button>
        </form>
    """)

def register_product(params):
    """ Add product based on given params """
    if len(params.keys()):
        name = get_param(params, "name")
        sku = get_param(params, "sku")
        price = get_param(params, "price")
        result = create_product(name, sku, price)
    else: 
        return get_template(f"""
                <h2>Erreur</h2>
                <code>La requête est vide</code>
            """)

    if isinstance(result, numbers.Number):
        return get_template(f"""
                <h2>Information: l'article {result} a été ajouté.</h2>
                <a href="/products">← Retourner à la page des articles</a>
            """)
    else:
        return get_template(f"""
                <h2>Erreur</h2>
                <code>{result}</code>
            """)
    
def remove_product(product_id):
    """ Remove product with given ID """
    result = delete_product(product_id)
    if result:
        return get_template(f"""
            <h2>Information: l'article {product_id} a été supprimé.</h2>
            <a href="/products">← Retourner à la page des articles</a>
        """)
    else:
        return get_template(f"""
                <h2>Erreur</h2>
                <code>{result}</code>
            """)