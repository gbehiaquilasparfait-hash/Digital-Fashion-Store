"""
Digital Fashion Store - Backend FastAPI
Serveur central partagé par toutes les plateformes
"""

from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File, Form, WebSocket, WebSocketDisconnect
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse
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

# CORS - Autoriser toutes les origines
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

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)

    def disconnect(self, websocket: WebSocket):
        if websocket in self.active_connections:
            self.active_connections.remove(websocket)

    async def broadcast(self, message: dict):
        dead = []
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception:
                dead.append(connection)
        for c in dead:
            self.disconnect(c)

manager = ConnectionManager()

# ─── Base de données ──────────────────────────────────────────────────────────
async def get_db():
    async with aiosqlite.connect(DATABASE_FILE) as db:
        db.row_factory = aiosqlite.Row
        yield db

async def init_db():
    try:
        async with aiosqlite.connect(DATABASE_FILE) as db:
            db.row_factory = aiosqlite.Row

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

            await db.execute("""
                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    icon TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

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

            await db.execute("""
                CREATE TABLE IF NOT EXISTS product_images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id TEXT REFERENCES products(id) ON DELETE CASCADE,
                    image_path TEXT NOT NULL,
                    is_main BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            await db.execute("""
                CREATE TABLE IF NOT EXISTS product_likes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT REFERENCES users(id),
                    product_id TEXT REFERENCES products(id),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, product_id)
                )
            """)

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

            await db.execute("""
                INSERT OR IGNORE INTO categories (name, description, icon) VALUES
                ('Robes', 'Robes élégantes et tendance', '👗'),
                ('Chaussures', 'Chaussures pour toutes occasions', '👠'),
                ('Sacs', 'Sacs et maroquinerie', '👜'),
                ('Accessoires', 'Bijoux et accessoires mode', '💍'),
                ('Vêtements Homme', 'Mode masculine contemporaine', '👔'),
                ('Sport & Casual', 'Tenues décontractées et sportswear', '👟')
            """)

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
    db: aiosqlite.Connection = Depends(get_db)
):
    if not credentials:
        raise HTTPException(status_code=401, detail="Authentification requise")
    payload = decode_token(credentials.credentials)
    cursor = await db.execute(
        "SELECT * FROM users WHERE id = ? AND is_active = 1", (payload["user_id"],)
    )
    user = await cursor.fetchone()
    if not user:
        raise HTTPException(status_code=401, detail="Utilisateur introuvable")
    return dict(user)

async def get_admin_user(user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Accès administrateur requis")
    return user

# ─── Routes Auth ──────────────────────────────────────────────────────────────
@app.post("/api/auth/register", tags=["Auth"])
async def register(data: UserRegister, db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("SELECT id FROM users WHERE phone = ?", (data.phone,))
    existing = await cursor.fetchone()
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
async def login(data: UserLogin, db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute(
        "SELECT * FROM users WHERE phone = ? AND is_active = 1", (data.phone,)
    )
    user = await cursor.fetchone()
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

# ─── Routes Catégories ────────────────────────────────────────────────────────
@app.get("/api/categories", tags=["Catégories"])
async def get_categories(db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("SELECT * FROM categories ORDER BY name")
    categories = await cursor.fetchall()
    return [dict(c) for c in categories]

# ─── Routes Produits ──────────────────────────────────────────────────────────
@app.get("/api/products", tags=["Produits"])
async def list_products(
    category_id: Optional[int] = None,
    search: Optional[str] = None,
    sort: str = "recent",
    page: int = 1,
    limit: int = 12,
    db: aiosqlite.Connection = Depends(get_db)
):
    offset = (page - 1) * limit
    where_clauses = ["p.is_active = 1"]
    params = []

    if category_id:
        where_clauses.append("p.category_id = ?")
        params.append(category_id)

    if search:
        where_clauses.append("p.name LIKE ?")
        params.append(f"%{search}%")

    where_sql = " AND ".join(where_clauses)
    sort_map = {
        "recent": "p.created_at DESC",
        "popular": "p.views DESC",
        "liked": "p.likes DESC",
        "price_asc": "p.price ASC",
        "price_desc": "p.price DESC",
    }
    order_sql = sort_map.get(sort, "p.created_at DESC")

    cursor = await db.execute(
        f"SELECT COUNT(*) FROM products p WHERE {where_sql}", params
    )
    total_row = await cursor.fetchone()
    total = total_row[0] if total_row else 0

    cursor = await db.execute(
        f"""SELECT p.*, c.name as category_name FROM products p
            LEFT JOIN categories c ON p.category_id = c.id
            WHERE {where_sql}
            ORDER BY {order_sql}
            LIMIT ? OFFSET ?""",
        params + [limit, offset]
    )
    products = await cursor.fetchall()

    result = []
    for p in products:
        p_dict = dict(p)
        img_cursor = await db.execute(
            "SELECT image_path, is_main FROM product_images WHERE product_id = ? ORDER BY is_main DESC",
            (p_dict["id"],)
        )
        images = await img_cursor.fetchall()
        p_dict["images"] = [dict(img) for img in images]
        p_dict["main_image"] = images[0]["image_path"] if images else None
        result.append(p_dict)

    return {
        "products": result,
        "total": total,
        "page": page,
        "pages": (total + limit - 1) // limit
    }

@app.get("/api/products/{product_id}", tags=["Produits"])
async def get_product(product_id: str, db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT p.*, c.name as category_name FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE p.id = ? AND p.is_active = 1
    """, (product_id,))
    product = await cursor.fetchone()

    if not product:
        raise HTTPException(status_code=404, detail="Produit introuvable")

    p_dict = dict(product)
    await db.execute("UPDATE products SET views = views + 1 WHERE id = ?", (product_id,))
    await db.commit()

    img_cursor = await db.execute(
        "SELECT image_path, is_main FROM product_images WHERE product_id = ? ORDER BY is_main DESC",
        (product_id,)
    )
    images = await img_cursor.fetchall()
    p_dict["images"] = [dict(img) for img in images]
    p_dict["main_image"] = images[0]["image_path"] if images else None

    return p_dict

@app.post("/api/products", tags=["Produits"])
async def create_product(
    product: ProductCreate,
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    product_id = str(uuid.uuid4())
    await db.execute("""
        INSERT INTO products (id, name, description, price, stock, category_id, wave_link)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (product_id, product.name, product.description, product.price,
          product.stock, product.category_id, product.wave_link))
    await db.commit()

    cursor = await db.execute("""
        SELECT p.*, c.name as category_name FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE p.id = ?
    """, (product_id,))
    new_product = await cursor.fetchone()

    # Notifier tous les clients connectés via WebSocket
    await manager.broadcast({
        "event": "product_added",
        "data": dict(new_product)
    })

    return dict(new_product)

@app.put("/api/products/{product_id}", tags=["Produits"])
async def update_product(
    product_id: str,
    product: ProductUpdate,
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute("SELECT id FROM products WHERE id = ?", (product_id,))
    existing = await cursor.fetchone()
    if not existing:
        raise HTTPException(status_code=404, detail="Produit introuvable")

    updates = {}
    if product.name is not None: updates["name"] = product.name
    if product.description is not None: updates["description"] = product.description
    if product.price is not None: updates["price"] = product.price
    if product.stock is not None: updates["stock"] = product.stock
    if product.category_id is not None: updates["category_id"] = product.category_id
    if product.wave_link is not None: updates["wave_link"] = product.wave_link
    if product.is_active is not None: updates["is_active"] = product.is_active

    if updates:
        updates["updated_at"] = datetime.utcnow().isoformat()
        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        await db.execute(
            f"UPDATE products SET {set_clause} WHERE id = ?",
            list(updates.values()) + [product_id]
        )
        await db.commit()

    cursor = await db.execute("""
        SELECT p.*, c.name as category_name FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE p.id = ?
    """, (product_id,))
    updated = await cursor.fetchone()
    return dict(updated)

@app.delete("/api/products/{product_id}", tags=["Produits"])
async def delete_product(
    product_id: str,
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute("SELECT id FROM products WHERE id = ?", (product_id,))
    existing = await cursor.fetchone()
    if not existing:
        raise HTTPException(status_code=404, detail="Produit introuvable")

    # Suppression douce (is_active = 0)
    await db.execute("UPDATE products SET is_active = 0 WHERE id = ?", (product_id,))
    await db.commit()
    return {"message": "Produit supprimé avec succès"}

@app.post("/api/products/{product_id}/images", tags=["Produits"])
async def upload_product_images(
    product_id: str,
    files: List[UploadFile] = File(...),
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute("SELECT id FROM products WHERE id = ?", (product_id,))
    existing = await cursor.fetchone()
    if not existing:
        raise HTTPException(status_code=404, detail="Produit introuvable")

    uploaded = []
    for i, file in enumerate(files):
        if not file.content_type.startswith("image/"):
            continue
        ext = file.filename.split(".")[-1] if "." in file.filename else "jpg"
        filename = f"{product_id}_{uuid.uuid4().hex[:8]}.{ext}"
        filepath = f"uploads/products/{filename}"

        with open(filepath, "wb") as f:
            content = await file.read()
            f.write(content)

        is_main = 1 if i == 0 else 0
        await db.execute(
            "INSERT INTO product_images (product_id, image_path, is_main) VALUES (?, ?, ?)",
            (product_id, filepath, is_main)
        )
        uploaded.append(filepath)

    await db.commit()
    return {"uploaded": uploaded, "count": len(uploaded)}

# ─── Routes Commandes ──────────────────────────────────────────────────────────
@app.post("/api/orders", tags=["Commandes"])
async def create_order(
    order_data: OrderCreate,
    user=Depends(get_current_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute("SELECT * FROM products WHERE id = ? AND is_active = 1", (order_data.product_id,))
    product = await cursor.fetchone()
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

    await db.execute(
        "UPDATE products SET stock = stock - ? WHERE id = ?",
        (order_data.quantity, order_data.product_id)
    )
    await db.commit()

    return {
        "order_id": order_id,
        "status": "en attente",
        "total_price": total_price,
        "wave_link": product_dict.get("wave_link") or "https://app.wave.com"
    }

@app.get("/api/orders/my", tags=["Commandes"])
async def get_user_orders(
    user=Depends(get_current_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute("""
        SELECT o.*, p.name as product_name FROM orders o
        LEFT JOIN products p ON o.product_id = p.id
        WHERE o.user_id = ?
        ORDER BY o.created_at DESC
    """, (user["id"],))
    orders = await cursor.fetchall()
    return [dict(o) for o in orders]

@app.get("/api/admin/orders", tags=["Admin"])
async def get_admin_orders(
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute("""
        SELECT o.*, p.name as product_name, u.phone as user_phone, u.full_name as client_name
        FROM orders o
        LEFT JOIN products p ON o.product_id = p.id
        LEFT JOIN users u ON o.user_id = u.id
        ORDER BY o.created_at DESC
    """)
    orders = await cursor.fetchall()
    return [dict(o) for o in orders]

@app.get("/api/admin/stats", tags=["Admin"])
async def get_admin_stats(
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    prod_cursor = await db.execute("SELECT COUNT(*) FROM products WHERE is_active = 1")
    prod_count = (await prod_cursor.fetchone())[0]

    order_cursor = await db.execute("SELECT COUNT(*), SUM(total_price) FROM orders")
    order_row = await order_cursor.fetchone()
    order_count = order_row[0] or 0
    revenue = order_row[1] or 0

    user_cursor = await db.execute("SELECT COUNT(*) FROM users WHERE role = 'client'")
    user_count = (await user_cursor.fetchone())[0]

    return {
        "products": prod_count,
        "orders": order_count,
        "users": user_count,
        "revenue": revenue
    }

# ─── Routes Likes ──────────────────────────────────────────────────────────────
@app.post("/api/products/{product_id}/like", tags=["Likes"])
async def like_product(
    product_id: str,
    user=Depends(get_current_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute(
        "SELECT id FROM product_likes WHERE user_id = ? AND product_id = ?",
        (user["id"], product_id)
    )
    existing = await cursor.fetchone()

    if existing:
        await db.execute(
            "DELETE FROM product_likes WHERE user_id = ? AND product_id = ?",
            (user["id"], product_id)
        )
        await db.execute("UPDATE products SET likes = MAX(0, likes - 1) WHERE id = ?", (product_id,))
        liked = False
    else:
        await db.execute(
            "INSERT INTO product_likes (user_id, product_id) VALUES (?, ?)",
            (user["id"], product_id)
        )
        await db.execute("UPDATE products SET likes = likes + 1 WHERE id = ?", (product_id,))
        liked = True

    await db.commit()

    cursor = await db.execute("SELECT likes FROM products WHERE id = ?", (product_id,))
    row = await cursor.fetchone()
    likes_count = row["likes"] if row else 0

    await manager.broadcast({
        "event": "product_liked",
        "data": {"product_id": product_id, "likes": likes_count}
    })

    return {"liked": liked, "likes": likes_count}

# ─── WebSocket ────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
    await manager.connect(websocket)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket)

# ─── Routes pages HTML ────────────────────────────────────────────────────────
@app.get("/admin")
async def admin_panel():
    """Sert le panneau d'administration"""
    if os.path.exists("admin.html"):
        return FileResponse("admin.html")
    raise HTTPException(status_code=404, detail="Fichier admin.html introuvable. Ajoutez-le au même dossier que main.py.")

@app.get("/")
async def accueil():
    """Sert la boutique frontend"""
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    return {"message": "Digital Fashion Store API", "status": "online", "docs": "/docs"}

@app.get("/api/health", tags=["System"])
async def health_check():
    return {"status": "🟢 API en ligne", "version": "1.0.0"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000, log_level="info")

