# Labo 02 – Architecture monolithique, ORM, CQRS, Persistance polyglotte, DDD
<img src="https://upload.wikimedia.org/wikipedia/commons/2/2a/Ets_quebec_logo.png" width="250">    
ÉTS - LOG430 - Architecture logicielle - Chargé de laboratoire: Gabriel C. Ullmann, Automne 2025.    

## 🎯 Objectifs d’apprentissage
- Comprendre ce qu’est une architecture monolithique à travers l’exemple d’une application de gestion de magasin.
- Comprendre et appliquer les patrons CQRS (Command Query Responsibility Segregation) pour séparer les opérations de lecture et d'écriture. 
- Comprendre et appliquer le CQRS avec une persistance polyglotte afin d’optimiser les opérations de lecture et d’écriture.
- Comprendre l’importance d’un ORM (Object-Relational Mapping) pour faciliter l’interaction avec les bases de données.
- Comprendre l'implementation des concepts clés du DDD (Domain-Driven Design)

## ⚙️ Setup
Dans ce laboratoire, nous continuerons à développer l'application de gestion de magasin que nous avons commencée dans le laboratoire 01. Maintenant l'application deviendra plus complexe puisqu’elle permettra la gestion des commandes, des articles et des utilisateurs dans une interface Web. 

> ⚠️ IMPORTANT : Avant de commencer le setup et les activités, veuillez lire la documentation architecturale dans le répertoire `/docs/arc42/docs.pdf`.

### 1. Clonez le dépôt
Créez votre propre dépôt à partir du dépôt gabarit (template). Vous pouvez modifier la visibilité pour la rendre privée si vous voulez.
```bash
git clone https://github.com/guteacher/log430-a25-labo2
cd log430-a25-labo2
```

Ensuite, clonez votre dépôt sur votre ordinateur et sur votre serveur de déploiement (ex. VM). **Veillez à ne pas cloner le dépôt d'origine**.

### 2. Créez un fichier .env
Créez un fichier `.env` basé sur `.env.example`. Dans le fichier `.env`, utilisez les mêmes identifiants que ceux mentionnés dans `docker-compose.yml`. Veuillez suivre la même approche que pour le laboratoire 01.

### 3. Ouvrez le port 5000 dans la conteneur
Les services dans le conteneur sont isolés par défaut. Dans le fichier `docker-compose.yml`, faites une correspondance entre le port 5000 du service `store_manager` et le port 5000 de votre ordinateur pour utiliser l'interface Web. 
```bash
ports:
    - "5000:5000"
```
> > 📝 **NOTE 1** : Si votre conteneur est dans une machine virtuelle et vous voulez accéder au port 5000 à partir de votre ordinateur de développement, il sera nécessaire également d'ouvrir la porte 5000 de la machine virtuelle à l'extérieur dans le pare-feu.

> > 📝 **NOTE 2** : Si, à tout moment, vous décidez d'exécuter l'application sur votre machine hôte plutôt que sur Docker, veillez à arrêter au préalable le service `store_manager` dans Docker. Sinon, votre application ne fonctionnera pas car le port 5000 est déjà occupé.

### 4. Préparez l’environnement de développement
Suivez les mêmes étapes que dans le laboratoire 01. La seule différence est que vous démarrerez le conteneur Docker en mode **non interactif**. Il s'agit d'une application Web, nous n'avons donc pas besoin d'interagir via la ligne de commande avec l'application.
```bash
docker compose build
docker compose up -d
```

### 5. Observez l'implementation du DDD
Dans l'application de gestion de magasin, nous retrouvons l’implémentation de plusieurs concepts clés du DDD que nous devons comprendre avant de commencer les activités :

- **Ubiquitous Language** : Les mêmes noms d'entités sont utilisés à la fois par les développeurs et les experts du domaine. Par exemple, des noms tels que Commande/Order, Article/Product, Utilisateur/User apparaissent à la fois dans la documentation, les diagrammes et le code.

- **Value Objects** : les modules du répertoire `models`, tels que `Order`, contiennent le « value object » `OrderItem`. Ce dernier n’a pas d’identité propre en dehors du contexte de `Order` (par exemple, un item de commande n’existe que dans une commande).

- **Aggregates** : les modules du répertoire `commands`, tels que `write_order.py`, assurent la cohérence transactionnelle des données dans MySQL. Par exemple, en cas d’erreur, `write_order.py` appelle la fonction `rollback()` dans SQLAlchemy pour annuler les opérations effectuées dans la base de données et éviter les incohérences. Si tout se passe bien, la méthode `commit()` est appelée afin de confirmer le changement d’état dans MySQL. La méthode `hset` dans Redis fonctionne également de manière cohérente.

- **Repositories** : les modules des répertoires `commands` et `queries`, comme `write_order.py` et `read_order.py`, jouent le rôle de `Repository` dans l'application de gestion de magasin. Ils fournissent des méthodes telles que `add`, `delete` et `get`, et masquent les opérations de base de données réalisées via SQLAlchemy, tout en maintenant la ségrégation entre lecture et écriture. Dans un projet non CQRS, nous pourrions créer un seul fichier `order_repository.py` contenant toutes les opérations.

Dans le cadre des activités, nous n'implémenterons pas directement les concepts DDD, mais nous utiliserons des modules qui les implémentent déjà, tels que `write_order.py`.

### 6. Préparez l’environnement de déploiement et le pipeline CI/CD
Utilisez les mêmes approches qui ont été abordées lors des laboratoires 00 et 01.

## 🧪 Activités pratiques

### 1. Population initiale de Redis au démarrage
Dans `commands/write_order.py`, la méthode `sync_all_orders_to_redis` charge toutes les commandes depuis MySQL vers Redis au démarrage de l'application. Veuillez terminer l'implémentation et assurez-vous qu'elle ne s'exécute qu'une seule fois au démarrage de l'application. Cette opération prendra plus de temps et de ressources à mesure que notre base de données se développe, nous voulons donc la faire uniquement lorsque cela est strictement nécessaire.

> 💡 **Question 1** : Lorsque l'application démarre, la synchronisation entre Redis et MySQL est-elle initialement déclenchée par quelle méthode ? Veuillez inclure le code pour illustrer votre réponse.

### 2. Modifiez la View de commandes pour utiliser uniquement Redis
Dans `views/order_view.py`, remplacez l'appel à `list_orders` par un appel à une autre méthode qui lit les commandes à partir de Redis. Veuillez terminer la méthode  `get_orders_from_redis` qui existe déjà dans `queries/read_order.py`.

> 💡 **Question 2** : Quelles méthodes avez-vous utilisées pour lire des données à partir de Redis ? Veuillez inclure le code pour illustrer votre réponse.

### 3. Insérez dans Redis
Dans `commands/write_order.py`, à chaque commande ajoutée dans MySQL, insérez-la également dans Redis. Même si cela peut paraître redondant, cela nous permettra de générer des rapports statistiques sur les commandes sans lire directement dans MySQL. Pour une application à forte charge (grand nombre de requêtes), cela permet de réduire la pression sur MySQL.

> 💡 **Question 3** : Quelles méthodes avez-vous utilisées pour ajouter des données dans Redis ? Veuillez inclure le code pour illustrer votre réponse.

### 4. Supprimez dans Redis
Toujours dans `commands/write_order.py`, à chaque commande supprimée de MySQL, supprimez-la également de Redis afin de maintenir la consistance des données.

> 💡 **Question 4** : Quelles méthodes avez-vous utilisées pour supprimer des données dans Redis ? Veuillez inclure le code pour illustrer votre réponse.

### 5. Créez un rapport : les plus gros acheteurs
Dans `queries/read_order.py`, créez une méthode qui obtient la liste des top 10 des utilisateurs ayant le plus dépensé en commandes. Les données que vous devez lire pour créer le rapport sont les mêmes que vous avez ajoutées à Redis dans l'activité 3. Utilisez la méthode `sorted` pour trier le résultat par total dépensé (ordre décroissant).

```python
expenses_by_user = defaultdict(float)
for order in orders:
    expenses_by_user[order.user_id] += order.total
highest_spending_users = sorted(expenses_by_user.items(), key=lambda item: item[1], reverse=True)
```

> 💡 **Question 5** : Si nous souhaitions créer un rapport similaire, mais présentant les produits les plus vendus, les informations dont nous disposons actuellement dans Redis sont-elles suffisantes, ou devrions-nous chercher dans les tables sur MySQL ? Si nécessaire, quelles informations devrions-nous ajouter à Redis ? Veuillez inclure le code pour illustrer votre réponse.

### 6. Créez un rapport : les articles plus vendus
Dans `queries/read_order.py`, créez une méthode qui obtient la liste des articles les plus vendus. Triez le résultat par nombre d'articles vendus (ordre décroissant). Pour obtenir les données nécessaires à ce rapport, gardez chaque article de la commande (`order_items`) synchronisé avec Redis. Utilisez la méthode `incr` pour mettre à jour la quantité vendue de chaque article à chaque fois qu'une nouvelle commande est ajoutée à MySQL. 

```python
r.incr("product:123", 1)
```

### ✅ Correction des activités

Des tests unitaires sont inclus dans le dépôt. Pour les exécuter :

```bash
python3 -m pytest
```

Si tous les tests passent ✅, vos implémentations sont correctes.

## 📦 Livrables
- Un fichier .zip contenant l’intégralité du code source du projet Labo 02.
- Une vidéo expliquant les principales observations, décisions et défis/difficultés/problèmes rencontrés durant l'étape 1.
    - Exigences : Maximum 5 minutes, format .mp4 ou .webm. 
    - Veuillez utiliser un outil tel que [Handbrake](https://handbrake.fr/) pour compresser la vidéo si elle dépasse 20 Mo.
- Un rapport en .pdf répondant aux 5 questions présentées dans ce document. Il est obligatoire d’illustrer vos réponses avec du code ou des captures de terminal.
