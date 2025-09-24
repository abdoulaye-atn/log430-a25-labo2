"""
User view
SPDX - License - Identifier: LGPL - 3.0 - or -later
Auteurs : Gabriel C. Ullmann, Fabio Petrillo, 2025
"""
import numbers
from views.template_view import get_template, get_param
from controllers.user_controller import create_user, delete_user, list_users

def show_user_form():
    """ Show user form and list """
    users = list_users(10)
    rows = [f"""
            <tr>
                <td>{user.id}</td>
                <td>{user.name}</td>
                <td><a href="/users/remove/{user.id}">Supprimer</a></td>
            </tr> """ for user in users]
    return get_template(f"""
        <h2>Utilisateurs</h2>
        <p>Voici les 10 derniers enregistrements :</p>
        <table class="table">
            <tr>
                <th>ID</th> 
                <th>Prénom</th>
                <th>Actions</th> 
            </tr>  
            {" ".join(rows)}
        </table>
        <h2>Enregistrement</h2>
        <form method="POST" action="/users/add">
            <div class="mb-3">
                <label class="form-label">Prénom</label>
                <input class="form-control"  type="text" name="name" maxlength="100" required>
            </div>
            <div class="mb-3">
                <label class="form-label">Adresse courriel</label>
                <input class="form-control"  type="email" name="email" maxlength="150" required>
            </div>
            <button type="submit" class="btn btn-primary">Enregistrer</button>
        </form>
    """)

def register_user(params):
    """ Add user based on params """
    if len(params.keys()):
        name = get_param(params, "name")
        email = get_param(params, "email")
        result = create_user(name, email)
    else: 
        return get_template(f"""
                <h2>Erreur</h2>
                <code>La requête est vide</code>
            """)

    if isinstance(result, numbers.Number):
        return get_template(f"""
                <h2>Information: l'utilisateur {result} a été ajouté.</h2>
                <a href="/users">← Retourner à la page des utilisateurs</a>
            """)
    else:
        return get_template(f"""
                <h2>Erreur</h2>
                <code>{result}</code>
            """)
    
def remove_user(user_id):
    """ Remove user with given ID """
    result = delete_user(user_id)
    if result:
        return get_template(f"""
            <h2>Information: l'utilisateur {user_id} a été supprimé.</h2>
            <a href="/users">← Retourner à la page des utilisateurs</a>
        """)
    else:
        return get_template(f"""
                <h2>Erreur</h2>
                <code>{result}</code>
            """)