# 👗 Digital Fashion Store

Plateforme e-commerce moderne pour la mode africaine avec FastAPI backend et interface web réactive.

## 🌟 Caractéristiques

- ✨ Interface web élégante et responsive
- 🔐 Authentification sécurisée (JWT)
- 🛍️ Catalogue de produits complet
- 💳 Intégration Wave Money pour les paiements
- 🌙 Mode sombre/clair
- 📱 Optimisé mobile
- 🔔 Système de notifications
- ❤️ Système de likes/favoris
- 🎯 Gestion des catégories

## 🚀 Démarrage rapide

### Prérequis
- Python 3.8+
- pip

### Installation

1. **Cloner le repository**
```bash
git clone https://github.com/gbehiaquilasparfait-hash/Digital-Fashion-Store.git
cd Digital-Fashion-Store
```

2. **Installer les dépendances**
```bash
pip install -r requirements.txt
```

3. **Lancer l'API**
```bash
python -m uvicorn serveur:app --reload --host 0.0.0.0 --port 3000
```

4. **Lancer le serveur web** (dans un nouveau terminal)
```bash
python -m http.server 8888 --directory .
```

5. **Accéder au site**
- Frontend: http://localhost:8888/commerce%20en%20ligne%20index.html
- API Docs: http://localhost:3000/docs

## 📋 Utilisation

### Compte Admin par défaut
- **Téléphone:** `00000000`
- **Mot de passe:** `Admin@1234`

### Endpoints API disponibles

#### Authentication
- `POST /api/auth/register` - Créer un compte
- `POST /api/auth/login` - Se connecter

#### Produits
- `GET /api/products` - Lister tous les produits
- `GET /api/products/{id}` - Détails d'un produit
- `GET /api/categories` - Lister les catégories

#### Commandes
- `POST /api/orders` - Créer une commande
- `GET /api/orders` - Mes commandes

#### Likes
- `POST /api/products/{id}/like` - Aimer/Retirer un like

#### Système
- `GET /api/health` - Vérifier l'état du serveur

## 📁 Structure du projet

```
.
├── serveur.py                          # Backend FastAPI
├── commerce en ligne index.html         # Frontend web
├── fashion_store.db                    # Base de données SQLite
├── uploads/                            # Stockage des images
├── requirements.txt                    # Dépendances
├── README.md                           # Ce fichier
└── .gitignore                          # Fichiers ignorés par Git
```

## 🗄️ Base de données

La plateforme utilise **SQLite3** avec les tables suivantes:
- `users` - Comptes utilisateurs
- `products` - Catalogue de produits
- `categories` - Catégories
- `orders` - Commandes
- `product_likes` - Favoris
- `notifications` - Notifications
- `product_images` - Images produits

## 🔐 Sécurité

- Mots de passe hashés avec bcrypt
- Tokens JWT pour l'authentification
- CORS configuré pour développement
- Validation des données avec Pydantic

## 🚀 Déploiement

### Sur Heroku
```bash
# Créer le Procfile (inclus)
# Déployer
git push heroku main
```

### Sur PythonAnywhere
1. Créer un compte sur pythonanywhere.com
2. Uploader les fichiers
3. Configurer l'application WSGI

## 🛠️ Technologies utilisées

- **Backend:** FastAPI, uvicorn, SQLite3
- **Authentification:** JWT, bcrypt
- **Frontend:** HTML5, CSS3, JavaScript vanilla
- **Paiements:** Wave Money API

## 📞 Support

Pour toute question ou problème, ouvrez une issue sur GitHub.

## 📄 Licence

Ce projet est sous licence MIT.

---

**Fait avec ❤️ pour la mode africaine**
