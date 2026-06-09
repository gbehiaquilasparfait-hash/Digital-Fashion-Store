"""
Digital Fashion Store - Backend FastAPI
Serveur central partagé par toutes les plateformes
"""

from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File, Form, WebSocket, WebSocketDisconnect
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse
import sqlite3
import bcrypt
import jwt
import json
import os
import uuid
import shutil
from datetime import datetime, timedelta
from typing import Optional, List
from pydantic import BaseModel
import asyncio
import aiosqlite

# ─── Configuration ───────────────────────────────────────────────────────────
SECRET_KEY = os.getenv("SECRET_KEY", "digital_fashion_store_secret_2024")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24 * 7  # 7 jours
DATABASE_FILE = "fashion_store.db"

app = FastAPI(
    title="Digital Fashion Store API",
    description="API REST pour la plateforme e-commerce Digital Fashion Store",
    version="1.0.0"
)

# CORS - Autoriser toutes les origines (dev)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Dossier pour les images
os.makedirs("uploads/products", exist_ok=True)
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")

security = HTTPBearer(auto_error=False)

# ─── WebSocket Manager ────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active_connections: List[WebSocket] = []
        self.admin_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        self.active_connections.discard(websocket)

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except:
                pass

    async def broadcast_to_admins(self, message: dict):
        for connection in self.admin_connections:
            try:
                await connection.send_json(message)
            except:
                pass

manager = ConnectionManager()

# ─── Base de données ──────────────────────────────────────────────────────────
async def get_db():
    async with aiosqlite.connect(DATABASE_FILE) as db:
        db.row_factory = aiosqlite.Row
        yield db

async def init_db():
    try:
        async with aiosqlite.connect(DATABASE_FILE) as db:
            # Table utilisateurs
            await db.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id TEXT PRIMARY KEY,
                    phone TEXT UNIQUE NOT NULL,
                    password_hash TEXT NOT NULL,
                    full_name TEXT,
                    email TEXT,
                    role TEXT DEFAULT 'client',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    is_active BOOLEAN DEFAULT 1
                )
            """)

            # Table catégories
            await db.execute("""
                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    icon TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Table produits
            await db.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    price REAL NOT NULL,
                    stock INTEGER DEFAULT 0,
                    category_id INTEGER REFERENCES categories(id),
                    wave_link TEXT,
                    views INTEGER DEFAULT 0,
                    likes INTEGER DEFAULT 0,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Table images produits
            await db.execute("""
                CREATE TABLE IF NOT EXISTS product_images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id TEXT REFERENCES products(id) ON DELETE CASCADE,
                    image_path TEXT NOT NULL,
                    is_main BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Table likes
            await db.execute("""
                CREATE TABLE IF NOT EXISTS product_likes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT REFERENCES users(id),
                    product_id TEXT REFERENCES products(id),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, product_id)
                )
            """)

            # Table commandes
            await db.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY,
                    user_id TEXT REFERENCES users(id),
                    product_id TEXT REFERENCES products(id),
                    quantity INTEGER DEFAULT 1,
                    total_price REAL NOT NULL,
                    status TEXT DEFAULT 'en attente',
                    payment_method TEXT DEFAULT 'wave',
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Table notifications
            await db.execute("""
                CREATE TABLE IF NOT EXISTS notifications (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT REFERENCES users(id),
                    title TEXT NOT NULL,
                    message TEXT,
                    type TEXT,
                    is_read BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Insérer des catégories par défaut
            await db.execute("""
                INSERT OR IGNORE INTO categories (name, description, icon) VALUES
                ('Robes', 'Robes élégantes et tendance', '👗'),
                ('Chaussures', 'Chaussures pour toutes occasions', '👠'),
                ('Sacs', 'Sacs et maroquinerie', '👜'),
                ('Accessoires', 'Bijoux et accessoires mode', '💍'),
                ('Vêtements Homme', 'Mode masculine contemporaine', '👔'),
                ('Sport & Casual', 'Tenues décontractées et sportswear', '👟')
            """)

            # Créer un compte admin par défaut
            admin_id = str(uuid.uuid4())
            admin_pass = bcrypt.hashpw("Admin@1234".encode(), bcrypt.gensalt()).decode()
            await db.execute("""
                INSERT OR IGNORE INTO users (id, phone, password_hash, full_name, role)
                VALUES (?, '00000000', ?, 'Administrateur', 'admin')
            """, (admin_id, admin_pass))

            await db.commit()
            print("✅ Base de données initialisée avec succès")
    except Exception as e:
        print(f"❌ Erreur initialisation DB: {e}")

@app.on_event("startup")
async def startup():
    await init_db()

# ─── Modèles Pydantic ─────────────────────────────────────────────────────────
class UserRegister(BaseModel):
    phone: str
    password: str
    full_name: Optional[str] = None
    email: Optional[str] = None

class UserLogin(BaseModel):
    phone: str
    password: str

class ProductCreate(BaseModel):
    name: str
    description: Optional[str] = None
    price: float
    stock: int = 0
    category_id: Optional[int] = None
    wave_link: Optional[str] = None

class ProductUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    price: Optional[float] = None
    stock: Optional[int] = None
    category_id: Optional[int] = None
    wave_link: Optional[str] = None
    is_active: Optional[bool] = None

class OrderCreate(BaseModel):
    product_id: str
    quantity: int = 1

# ─── Utilitaires JWT ──────────────────────────────────────────────────────────
def create_token(user_id: str, phone: str, role: str) -> str:
    payload = {
        "user_id": user_id,
        "phone": phone,
        "role": role,
        "exp": datetime.utcnow() + timedelta(hours=ACCESS_TOKEN_EXPIRE_HOURS)
    }
    return jwt.encode(payload, SECRET_KEY, algorithm=ALGORITHM)

def decode_token(token: str) -> dict:
    try:
        return jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
    except jwt.ExpiredSignatureError:
        raise HTTPException(status_code=401, detail="Token expiré")
    except jwt.InvalidTokenError:
        raise HTTPException(status_code=401, detail="Token invalide")

async def get_current_user(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db=Depends(get_db)
):
    if not credentials:
        raise HTTPException(status_code=401, detail="Authentification requise")
    payload = decode_token(credentials.credentials)
    user = await db.fetchone("SELECT * FROM users WHERE id = ? AND is_active = TRUE", (payload["user_id"],))
    if not user:
        raise HTTPException(status_code=401, detail="Utilisateur introuvable")
    return dict(user)

async def get_admin_user(user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Accès administrateur requis")
    return user

# ─── Routes Auth ──────────────────────────────────────────────────────────────
@app.post("/api/auth/register", tags=["Auth"])
async def register(data: UserRegister, db=Depends(get_db)):
    existing = await db.fetchone("SELECT id FROM users WHERE phone = ?", (data.phone,))
    if existing:
        raise HTTPException(status_code=400, detail="Numéro déjà enregistré")

    user_id = str(uuid.uuid4())
    hashed = bcrypt.hashpw(data.password.encode(), bcrypt.gensalt()).decode()
    await db.execute("""
        INSERT INTO users (id, phone, password_hash, full_name, email)
        VALUES (?, ?, ?, ?, ?)
    """, (user_id, data.phone, hashed, data.full_name, data.email))
    await db.commit()

    token = create_token(user_id, data.phone, "client")
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": user_id,
            "phone": data.phone,
            "full_name": data.full_name,
            "role": "client"
        }
    }

@app.post("/api/auth/login", tags=["Auth"])
async def login(data: UserLogin, db=Depends(get_db)):
    user = await db.fetchone("SELECT * FROM users WHERE phone = ? AND is_active = TRUE", (data.phone,))
    if not user:
        raise HTTPException(status_code=401, detail="Identifiants invalides")

    user_dict = dict(user)
    if not bcrypt.checkpw(data.password.encode(), user_dict["password_hash"].encode()):
        raise HTTPException(status_code=401, detail="Identifiants invalides")

    token = create_token(user_dict["id"], user_dict["phone"], user_dict["role"])
    return {
        "access_token": token,
        "token_type": "bearer",
        "user": {
            "id": user_dict["id"],
            "phone": user_dict["phone"],
            "full_name": user_dict["full_name"],
            "role": user_dict["role"]
        }
    }

# ─── Routes Produits ──────────────────────────────────────────────────────────
@app.get("/api/products", tags=["Produits"])
async def list_products(db=Depends(get_db)):
    products = await db.fetchall("""
        SELECT p.*, c.name as category_name FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE p.is_active = TRUE
        ORDER BY p.created_at DESC
    """)
    result = []
    for p in products:
        p_dict = dict(p)
        images = await db.fetchall("SELECT image_path FROM product_images WHERE product_id = ? ORDER BY is_main DESC", (p_dict["id"],))
        p_dict["images"] = [dict(img) for img in images]
        if images:
            p_dict["main_image"] = images[0]["image_path"] if images else None
        result.append(p_dict)
    return result

@app.get("/api/products/{product_id}", tags=["Produits"])
async def get_product(product_id: str, db=Depends(get_db)):
    product = await db.fetchone("""
        SELECT p.*, c.name as category_name FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE p.id = ? AND p.is_active = TRUE
    """, (product_id,))
    
    if not product:
        raise HTTPException(status_code=404, detail="Produit introuvable")
    
    p_dict = dict(product)
    await db.execute("UPDATE products SET views = views + 1 WHERE id = ?", (product_id,))
    await db.commit()
    
    images = await db.fetchall("SELECT image_path FROM product_images WHERE product_id = ? ORDER BY is_main DESC", (product_id,))
    p_dict["images"] = [dict(img) for img in images]
    if images:
        p_dict["main_image"] = images[0]["image_path"]
    
    return p_dict

@app.get("/api/categories", tags=["Catégories"])
async def get_categories(db=Depends(get_db)):
    categories = await db.fetchall("SELECT * FROM categories ORDER BY name")
    return [dict(c) for c in categories]

# ─── Routes Commandes ──────────────────────────────────────────────────────────
@app.post("/api/orders", tags=["Commandes"])
async def create_order(order_data: OrderCreate, user=Depends(get_current_user), db=Depends(get_db)):
    product = await db.fetchone("SELECT * FROM products WHERE id = ?", (order_data.product_id,))
    if not product:
        raise HTTPException(status_code=404, detail="Produit introuvable")
    
    product_dict = dict(product)
    if product_dict["stock"] < order_data.quantity:
        raise HTTPException(status_code=400, detail="Stock insuffisant")

    order_id = str(uuid.uuid4())
    total_price = product_dict["price"] * order_data.quantity
    
    await db.execute("""
        INSERT INTO orders (id, user_id, product_id, quantity, total_price, status, payment_method)
        VALUES (?, ?, ?, ?, ?, 'en attente', 'wave')
    """, (order_id, user["id"], order_data.product_id, order_data.quantity, total_price))
    
    await db.execute("UPDATE products SET stock = stock - ? WHERE id = ?", (order_data.quantity, order_data.product_id))
    await db.commit()

    return {
        "order_id": order_id,
        "status": "en attente",
        "total_price": total_price,
        "wave_link": product_dict.get("wave_link", "https://app.wave.com")
    }

@app.get("/api/orders", tags=["Commandes"])
async def get_user_orders(user=Depends(get_current_user), db=Depends(get_db)):
    orders = await db.fetchall("""
        SELECT o.*, p.name as product_name, p.price FROM orders o
        LEFT JOIN products p ON o.product_id = p.id
        WHERE o.user_id = ?
        ORDER BY o.created_at DESC
    """, (user["id"],))
    return [dict(o) for o in orders]

# ─── Routes Likes ──────────────────────────────────────────────────────────────
@app.post("/api/products/{product_id}/like", tags=["Likes"])
async def like_product(product_id: str, user=Depends(get_current_user), db=Depends(get_db)):
    existing = await db.fetchone(
        "SELECT id FROM product_likes WHERE user_id = ? AND product_id = ?",
        (user["id"], product_id)
    )
    
    if existing:
        await db.execute("DELETE FROM product_likes WHERE user_id = ? AND product_id = ?", (user["id"], product_id))
        await db.execute("UPDATE products SET likes = likes - 1 WHERE id = ?", (product_id,))
        liked = False
    else:
        like_id = str(uuid.uuid4())
        await db.execute("""
            INSERT INTO product_likes (user_id, product_id)
            VALUES (?, ?)
        """, (user["id"], product_id))
        await db.execute("UPDATE products SET likes = likes + 1 WHERE id = ?", (product_id,))
        liked = True
    
    await db.commit()
    return {"liked": liked}

# ─── Route Santé ──────────────────────────────────────────────────────────────
@app.get("/")
async def accueil():
    return FileResponse("index.html")

@app.get("/api/health", tags=["System"])
async def health_check():
    return {"status": "🟢 API en ligne", "version": "1.0.0"}
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000, log_level="info")
