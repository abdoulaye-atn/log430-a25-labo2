"""
Template view
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""

from controllers.order_controller import populate_redis_from_mysql

def show_main_menu():
    """ Show main menu, populate Redis if needed """
    populate_redis_from_mysql()
    return get_template("""
        <nav>
            <h2>Formulaires d'enregistrement</h2>
            <ul class="list-group">
                <li class="list-group-item"><a href="/users">Utilisateurs</a></li>
                <li class="list-group-item"><a href="/products">Articles</a></li>
                <li class="list-group-item"><a href="/orders">Commandes</a></li>
            </ul>
            <br>
            <h2>Rapports</h2>        
            <ul class="list-group">
                <li class="list-group-item"><a href="/orders/reports/highest_spenders">Les plus gros acheteurs</a></li>
                <li class="list-group-item"><a href="/orders/reports/best_sellers">Les articles les plus vendus</a></li>
            </ul>
        </nav>""", homepage=True)

def show_404_page():
    """ Show 404 page """
    return get_template("<h2>404 Page Not Found</h2><p>Desolé, la page que vous recherchez semble introuvable.<p>")

def get_param(params, name):
    """ Get and sanitize paramters from request """
    if not params or not name or not params.get(name):
        return ""
    return params.get(name)[0]

def get_template(content, homepage=False):
    """ Inject content into base HTML template for the application """
    breadcrumb_text = """<p>Application de gestion de magasins</p>""" if homepage else """<a href="/">← Retourner à la page d'accueil</a>"""
    return f"""<!DOCTYPE html>
    <html lang="fr">
        <head>
            <meta charset="UTF-8">
            <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.3.8/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-sRIl4kxILFvY47J16cr9ZwB07vP4J8+LH7qKQnuqkuIAvNWLzeN8tE5YBujZqJLB" crossorigin="anonymous">
            <link rel="stylesheet" href="/assets/light.css">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body>
            <header>
                <img id="logo" src="/assets/logo.svg" />
                <h1>Le Magasin du Coin</h1>
            </header>
            <div id="breadcrumbs">
                {breadcrumb_text}
            </div>
            <hr>
            {content}
        </body>
    </html>
    """