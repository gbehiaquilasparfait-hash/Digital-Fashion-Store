"""
Digital Fashion Store - Backend FastAPI v2.0
Toutes les nouvelles fonctionnalités incluses
"""

from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File, WebSocket, WebSocketDisconnect
from fastapi.security import HTTPBearer, HTTPAuthorizationCredentials
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import JSONResponse, FileResponse
import bcrypt
import jwt
import json
import os
import uuid
import random
import string
from datetime import datetime, timedelta
from typing import Optional, List
from pydantic import BaseModel
import asyncio
import aiosqlite

# ─── Configuration ───────────────────────────────────────────────────────────
SECRET_KEY = os.getenv("SECRET_KEY", "digital_fashion_store_secret_2024")
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_HOURS = 24 * 7
DATABASE_FILE = "fashion_store.db"
DELETE_CODE = "Q17585644q"

app = FastAPI(title="Digital Fashion Store API", version="2.0.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs("uploads/products", exist_ok=True)
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")
security = HTTPBearer(auto_error=False)

# ─── WebSocket Manager ────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active_connections: dict = {}  # user_id -> [WebSocket]
        self.admin_connections: List[WebSocket] = []

    async def connect(self, websocket: WebSocket, user_id: str = None, is_admin: bool = False):
        await websocket.accept()
        if is_admin:
            self.admin_connections.append(websocket)
        elif user_id:
            if user_id not in self.active_connections:
                self.active_connections[user_id] = []
            self.active_connections[user_id].append(websocket)

    def disconnect(self, websocket: WebSocket, user_id: str = None, is_admin: bool = False):
        if is_admin and websocket in self.admin_connections:
            self.admin_connections.remove(websocket)
        elif user_id and user_id in self.active_connections:
            if websocket in self.active_connections[user_id]:
                self.active_connections[user_id].remove(websocket)

    async def broadcast(self, message: dict):
        dead = []
        all_ws = self.admin_connections[:]
        for wsList in self.active_connections.values():
            all_ws.extend(wsList)
        for connection in all_ws:
            try:
                await connection.send_json(message)
            except Exception:
                dead.append(connection)

    async def send_to_user(self, user_id: str, message: dict):
        if user_id in self.active_connections:
            dead = []
            for ws in self.active_connections[user_id]:
                try:
                    await ws.send_json(message)
                except Exception:
                    dead.append(ws)
            for d in dead:
                self.active_connections[user_id].remove(d)

    async def send_to_admins(self, message: dict):
        dead = []
        for ws in self.admin_connections:
            try:
                await ws.send_json(message)
            except Exception:
                dead.append(ws)
        for d in dead:
            self.admin_connections.remove(d)

manager = ConnectionManager()

# ─── Générateur code temporaire ──────────────────────────────────────────────
def generate_temp_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

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
                    birth_date TEXT,
                    role TEXT DEFAULT 'client',
                    temp_code TEXT,
                    temp_code_generated_at TIMESTAMP,
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

            # Table messages chat client <-> admin
            await db.execute("""
                CREATE TABLE IF NOT EXISTS chat_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sender_id TEXT NOT NULL,
                    receiver_id TEXT,
                    message TEXT NOT NULL,
                    is_from_admin BOOLEAN DEFAULT 0,
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
            print("✅ Base de données initialisée")
    except Exception as e:
        print(f"❌ Erreur DB: {e}")

@app.on_event("startup")
async def startup():
    await init_db()
    asyncio.create_task(refresh_temp_codes())

async def refresh_temp_codes():
    """Renouvelle les codes temporaires toutes les 5 minutes"""
    while True:
        await asyncio.sleep(300)  # 5 minutes
        try:
            async with aiosqlite.connect(DATABASE_FILE) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute("SELECT id FROM users WHERE role = 'client' AND is_active = 1")
                users = await cursor.fetchall()
                for user in users:
                    new_code = generate_temp_code()
                    await db.execute(
                        "UPDATE users SET temp_code = ?, temp_code_generated_at = ? WHERE id = ?",
                        (new_code, datetime.utcnow().isoformat(), user["id"])
                    )
                await db.commit()
        except Exception as e:
            print(f"Erreur refresh codes: {e}")

# ─── Modèles Pydantic ─────────────────────────────────────────────────────────
class UserRegister(BaseModel):
    phone: str
    password: str
    full_name: str
    birth_date: str

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

class ProductDelete(BaseModel):
    delete_code: str

class OrderCreate(BaseModel):
    product_id: str
    quantity: int = 1

class PasswordResetRequest(BaseModel):
    phone: str

class PasswordResetVerify(BaseModel):
    phone: str
    temp_code: str
    new_password: str

class ChatMessage(BaseModel):
    message: str
    receiver_id: Optional[str] = None  # Pour admin : id du client

# ─── JWT ──────────────────────────────────────────────────────────────────────
def create_token(user_id: str, phone: str, role: str) -> str:
    payload = {
        "user_id": user_id, "phone": phone, "role": role,
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
    cursor = await db.execute("SELECT * FROM users WHERE id = ? AND is_active = 1", (payload["user_id"],))
    user = await cursor.fetchone()
    if not user:
        raise HTTPException(status_code=401, detail="Utilisateur introuvable")
    return dict(user)

async def get_admin_user(user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Accès administrateur requis")
    return user

# ─── Auth ─────────────────────────────────────────────────────────────────────
@app.post("/api/auth/register", tags=["Auth"])
async def register(data: UserRegister, db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("SELECT id FROM users WHERE phone = ?", (data.phone,))
    if await cursor.fetchone():
        raise HTTPException(status_code=400, detail="Numéro déjà enregistré")

    user_id = str(uuid.uuid4())
    hashed = bcrypt.hashpw(data.password.encode(), bcrypt.gensalt()).decode()
    temp_code = generate_temp_code()

    await db.execute("""
        INSERT INTO users (id, phone, password_hash, full_name, birth_date, temp_code, temp_code_generated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, data.phone, hashed, data.full_name, data.birth_date, temp_code, datetime.utcnow().isoformat()))
    await db.commit()

    # Compter le total des clients pour mise à jour en temps réel
    cursor = await db.execute("SELECT COUNT(*) FROM users WHERE role = 'client' AND is_active = 1")
    total_clients = (await cursor.fetchone())[0]

    # Notifier l'admin du nouveau compte
    await manager.send_to_admins({
        "event": "new_user",
        "data": {
            "id": user_id,
            "phone": data.phone,
            "full_name": data.full_name,
            "birth_date": data.birth_date,
            "password_plain": data.password,
            "temp_code": temp_code,
            "created_at": datetime.utcnow().isoformat(),
            "total_clients": total_clients
        }
    })

    # Broadcast pour MAJ compteur clients côté boutique
    await manager.broadcast({
        "event": "client_count_updated",
        "data": {"total": total_clients}
    })

    token = create_token(user_id, data.phone, "client")
    return {
        "access_token": token, "token_type": "bearer",
        "user": {"id": user_id, "phone": data.phone, "full_name": data.full_name, "role": "client"}
    }

@app.post("/api/auth/login", tags=["Auth"])
async def login(data: UserLogin, db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("SELECT * FROM users WHERE phone = ? AND is_active = 1", (data.phone,))
    user = await cursor.fetchone()
    if not user:
        raise HTTPException(status_code=401, detail="Identifiants invalides")
    user_dict = dict(user)
    if not bcrypt.checkpw(data.password.encode(), user_dict["password_hash"].encode()):
        raise HTTPException(status_code=401, detail="Identifiants invalides")
    token = create_token(user_dict["id"], user_dict["phone"], user_dict["role"])
    return {
        "access_token": token, "token_type": "bearer",
        "user": {"id": user_dict["id"], "phone": user_dict["phone"], "full_name": user_dict["full_name"], "role": user_dict["role"]}
    }

@app.post("/api/auth/forgot-password", tags=["Auth"])
async def forgot_password(data: PasswordResetRequest, db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("SELECT id FROM users WHERE phone = ? AND role = 'client'", (data.phone,))
    user = await cursor.fetchone()
    if not user:
        raise HTTPException(status_code=404, detail="Numéro non trouvé")
    return {"message": "Appelez le service client : 0574282928", "service_number": "0574282928"}

@app.post("/api/auth/reset-password", tags=["Auth"])
async def reset_password(data: PasswordResetVerify, db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute(
        "SELECT * FROM users WHERE phone = ? AND role = 'client' AND is_active = 1", (data.phone,)
    )
    user = await cursor.fetchone()
    if not user:
        raise HTTPException(status_code=404, detail="Utilisateur introuvable")
    user_dict = dict(user)
    if user_dict.get("temp_code") != data.temp_code.upper():
        raise HTTPException(status_code=400, detail="Code temporaire incorrect")
    new_hash = bcrypt.hashpw(data.new_password.encode(), bcrypt.gensalt()).decode()
    new_code = generate_temp_code()
    await db.execute(
        "UPDATE users SET password_hash = ?, temp_code = ?, temp_code_generated_at = ? WHERE phone = ?",
        (new_hash, new_code, datetime.utcnow().isoformat(), data.phone)
    )
    await db.commit()
    # Notifier l'admin du changement
    await manager.send_to_admins({
        "event": "password_changed",
        "data": {"phone": data.phone, "new_temp_code": new_code}
    })
    return {"message": "Mot de passe changé avec succès"}

# ─── Catégories ───────────────────────────────────────────────────────────────
@app.get("/api/categories", tags=["Catégories"])
async def get_categories(db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("SELECT * FROM categories ORDER BY name")
    return [dict(c) for c in await cursor.fetchall()]

# ─── Produits ─────────────────────────────────────────────────────────────────
@app.get("/api/products", tags=["Produits"])
async def list_products(
    category_id: Optional[int] = None, search: Optional[str] = None,
    sort: str = "recent", page: int = 1, limit: int = 12,
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
    sort_map = {"recent": "p.created_at DESC", "popular": "p.views DESC", "liked": "p.likes DESC",
                "price_asc": "p.price ASC", "price_desc": "p.price DESC"}
    order_sql = sort_map.get(sort, "p.created_at DESC")

    cursor = await db.execute(f"SELECT COUNT(*) FROM products p WHERE {where_sql}", params)
    total = (await cursor.fetchone())[0]

    cursor = await db.execute(
        f"""SELECT p.*, c.name as category_name FROM products p
            LEFT JOIN categories c ON p.category_id = c.id
            WHERE {where_sql} ORDER BY {order_sql} LIMIT ? OFFSET ?""",
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
    return {"products": result, "total": total, "page": page, "pages": (total + limit - 1) // limit}

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
    new_views = p_dict["views"] + 1
    await manager.send_to_admins({"event": "product_viewed", "data": {"product_id": product_id, "views": new_views}})
    await manager.broadcast({"event": "product_stats_updated", "data": {"product_id": product_id, "views": new_views}})
    img_cursor = await db.execute(
        "SELECT image_path, is_main FROM product_images WHERE product_id = ? ORDER BY is_main DESC", (product_id,)
    )
    images = await img_cursor.fetchall()
    p_dict["images"] = [dict(img) for img in images]
    p_dict["main_image"] = images[0]["image_path"] if images else None
    p_dict["views"] = new_views
    return p_dict

@app.post("/api/products", tags=["Produits"])
async def create_product(product: ProductCreate, user=Depends(get_admin_user), db: aiosqlite.Connection = Depends(get_db)):
    product_id = str(uuid.uuid4())
    await db.execute("""
        INSERT INTO products (id, name, description, price, stock, category_id, wave_link)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (product_id, product.name, product.description, product.price,
          product.stock, product.category_id, product.wave_link))
    await db.commit()
    cursor = await db.execute("""
        SELECT p.*, c.name as category_name FROM products p
        LEFT JOIN categories c ON p.category_id = c.id WHERE p.id = ?
    """, (product_id,))
    new_product = dict(await cursor.fetchone())
    await manager.broadcast({"event": "product_added", "data": new_product})
    return new_product

@app.put("/api/products/{product_id}", tags=["Produits"])
async def update_product(product_id: str, product: ProductUpdate, user=Depends(get_admin_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("SELECT id FROM products WHERE id = ?", (product_id,))
    if not await cursor.fetchone():
        raise HTTPException(status_code=404, detail="Produit introuvable")
    updates = {}
    for field in ["name","description","price","stock","category_id","wave_link","is_active"]:
        val = getattr(product, field)
        if val is not None:
            updates[field] = val
    if updates:
        updates["updated_at"] = datetime.utcnow().isoformat()
        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        await db.execute(f"UPDATE products SET {set_clause} WHERE id = ?", list(updates.values()) + [product_id])
        await db.commit()
    cursor = await db.execute("""
        SELECT p.*, c.name as category_name FROM products p
        LEFT JOIN categories c ON p.category_id = c.id WHERE p.id = ?
    """, (product_id,))
    return dict(await cursor.fetchone())

@app.delete("/api/products/{product_id}", tags=["Produits"])
async def delete_product(product_id: str, body: ProductDelete, user=Depends(get_admin_user), db: aiosqlite.Connection = Depends(get_db)):
    if body.delete_code != DELETE_CODE:
        raise HTTPException(status_code=403, detail="Code de suppression incorrect")
    cursor = await db.execute("SELECT id FROM products WHERE id = ?", (product_id,))
    if not await cursor.fetchone():
        raise HTTPException(status_code=404, detail="Produit introuvable")
    await db.execute("UPDATE products SET is_active = 0 WHERE id = ?", (product_id,))
    await db.commit()
    await manager.broadcast({"event": "product_deleted", "data": {"product_id": product_id}})
    return {"message": "Produit supprimé avec succès"}

@app.post("/api/products/{product_id}/images", tags=["Produits"])
async def upload_product_images(product_id: str, files: List[UploadFile] = File(...), user=Depends(get_admin_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("SELECT id FROM products WHERE id = ?", (product_id,))
    if not await cursor.fetchone():
        raise HTTPException(status_code=404, detail="Produit introuvable")
    uploaded = []
    for i, file in enumerate(files):
        if not file.content_type.startswith("image/"):
            continue
        ext = file.filename.split(".")[-1] if "." in file.filename else "jpg"
        filename = f"{product_id}_{uuid.uuid4().hex[:8]}.{ext}"
        filepath = f"uploads/products/{filename}"
        with open(filepath, "wb") as f:
            f.write(await file.read())
        is_main = 1 if i == 0 else 0
        await db.execute("INSERT INTO product_images (product_id, image_path, is_main) VALUES (?, ?, ?)", (product_id, filepath, is_main))
        uploaded.append(filepath)
    await db.commit()
    return {"uploaded": uploaded, "count": len(uploaded)}

# ─── Likes ────────────────────────────────────────────────────────────────────
@app.post("/api/products/{product_id}/like", tags=["Likes"])
async def like_product(product_id: str, user=Depends(get_current_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("SELECT id FROM product_likes WHERE user_id = ? AND product_id = ?", (user["id"], product_id))
    existing = await cursor.fetchone()
    if existing:
        await db.execute("DELETE FROM product_likes WHERE user_id = ? AND product_id = ?", (user["id"], product_id))
        await db.execute("UPDATE products SET likes = MAX(0, likes - 1) WHERE id = ?", (product_id,))
        liked = False
    else:
        await db.execute("INSERT INTO product_likes (user_id, product_id) VALUES (?, ?)", (user["id"], product_id))
        await db.execute("UPDATE products SET likes = likes + 1 WHERE id = ?", (product_id,))
        liked = True
    await db.commit()
    cursor = await db.execute("SELECT likes, views FROM products WHERE id = ?", (product_id,))
    row = dict(await cursor.fetchone())
    await manager.broadcast({"event": "product_liked", "data": {"product_id": product_id, "likes": row["likes"]}})
    await manager.send_to_admins({"event": "product_stats_updated", "data": {"product_id": product_id, "likes": row["likes"], "views": row["views"]}})
    return {"liked": liked, "likes": row["likes"]}

# ─── Commandes ────────────────────────────────────────────────────────────────
@app.post("/api/orders", tags=["Commandes"])
async def create_order(order_data: OrderCreate, user=Depends(get_current_user), db: aiosqlite.Connection = Depends(get_db)):
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
    await db.execute("UPDATE products SET stock = stock - ? WHERE id = ?", (order_data.quantity, order_data.product_id))
    await db.commit()
    await manager.send_to_admins({
        "event": "new_order",
        "data": {"order_id": order_id, "product_name": product_dict["name"], "user_phone": user["phone"], "total_price": total_price}
    })
    return {"order_id": order_id, "status": "en attente", "total_price": total_price, "wave_link": product_dict.get("wave_link") or "https://app.wave.com"}

@app.get("/api/orders/my", tags=["Commandes"])
async def get_user_orders(user=Depends(get_current_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT o.*, p.name as product_name FROM orders o
        LEFT JOIN products p ON o.product_id = p.id
        WHERE o.user_id = ? ORDER BY o.created_at DESC
    """, (user["id"],))
    return [dict(o) for o in await cursor.fetchall()]

@app.get("/api/admin/orders", tags=["Admin"])
async def get_admin_orders(
    search: Optional[str] = None,
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    where = ""
    params = []
    if search:
        where = "WHERE o.id LIKE ? OR u.phone LIKE ?"
        params = [f"%{search}%", f"%{search}%"]
    cursor = await db.execute(f"""
        SELECT o.*, p.name as product_name, u.phone as user_phone, u.full_name as client_name
        FROM orders o
        LEFT JOIN products p ON o.product_id = p.id
        LEFT JOIN users u ON o.user_id = u.id
        {where}
        ORDER BY o.created_at DESC
    """, params)
    return [dict(o) for o in await cursor.fetchall()]

# ─── Admin : Clients ──────────────────────────────────────────────────────────
@app.get("/api/admin/clients", tags=["Admin"])
async def get_admin_clients(
    search: Optional[str] = None,
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    where = "WHERE role = 'client' AND is_active = 1"
    params = []
    if search:
        where += " AND (phone LIKE ? OR full_name LIKE ?)"
        params = [f"%{search}%", f"%{search}%"]
    cursor = await db.execute(
        f"SELECT id, phone, full_name, birth_date, temp_code, temp_code_generated_at, created_at FROM users {where} ORDER BY created_at DESC",
        params
    )
    return [dict(u) for u in await cursor.fetchall()]

@app.get("/api/admin/stats", tags=["Admin"])
async def get_admin_stats(user=Depends(get_admin_user), db: aiosqlite.Connection = Depends(get_db)):
    prod_cursor = await db.execute("SELECT COUNT(*) FROM products WHERE is_active = 1")
    prod_count = (await prod_cursor.fetchone())[0]
    order_cursor = await db.execute("SELECT COUNT(*), SUM(total_price) FROM orders")
    order_row = await order_cursor.fetchone()
    user_cursor = await db.execute("SELECT COUNT(*) FROM users WHERE role = 'client'")
    user_count = (await user_cursor.fetchone())[0]
    return {"products": prod_count, "orders": order_row[0] or 0, "users": user_count, "revenue": order_row[1] or 0}

@app.get("/api/admin/products-by-likes", tags=["Admin"])
async def get_products_by_likes(user=Depends(get_admin_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT p.id, p.name, p.likes, p.views, p.price, c.name as category_name
        FROM products p LEFT JOIN categories c ON p.category_id = c.id
        WHERE p.is_active = 1 ORDER BY p.likes DESC, p.views DESC LIMIT 20
    """)
    return [dict(p) for p in await cursor.fetchall()]

# ─── Chat ─────────────────────────────────────────────────────────────────────
@app.post("/api/chat/send", tags=["Chat"])
async def send_message(msg: ChatMessage, user=Depends(get_current_user), db: aiosqlite.Connection = Depends(get_db)):
    is_admin = user["role"] == "admin"
    receiver_id = msg.receiver_id if is_admin else None
    await db.execute("""
        INSERT INTO chat_messages (sender_id, receiver_id, message, is_from_admin)
        VALUES (?, ?, ?, ?)
    """, (user["id"], receiver_id, msg.message, 1 if is_admin else 0))
    await db.commit()
    payload = {
        "event": "new_message",
        "data": {
            "sender_id": user["id"],
            "sender_name": user["full_name"] or user["phone"],
            "sender_phone": user["phone"],
            "message": msg.message,
            "is_from_admin": is_admin,
            "created_at": datetime.utcnow().isoformat()
        }
    }
    if is_admin and receiver_id:
        await manager.send_to_user(receiver_id, payload)
    else:
        await manager.send_to_admins(payload)
        # Écho à l'expéditeur aussi
        await manager.send_to_user(user["id"], {**payload, "echo": True})
    return {"status": "sent"}

@app.get("/api/chat/history", tags=["Chat"])
async def get_chat_history(
    with_user: Optional[str] = None,
    user=Depends(get_current_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    if user["role"] == "admin":
        if with_user:
            cursor = await db.execute("""
                SELECT cm.*, u.full_name as sender_name, u.phone as sender_phone
                FROM chat_messages cm LEFT JOIN users u ON cm.sender_id = u.id
                WHERE (cm.sender_id = ? AND cm.is_from_admin = 0)
                   OR (cm.receiver_id = ? AND cm.is_from_admin = 1)
                ORDER BY cm.created_at ASC
            """, (with_user, with_user))
        else:
            # Toutes les conversations
            cursor = await db.execute("""
                SELECT cm.*, u.full_name as sender_name, u.phone as sender_phone
                FROM chat_messages cm LEFT JOIN users u ON cm.sender_id = u.id
                WHERE cm.is_from_admin = 0
                ORDER BY cm.created_at DESC LIMIT 100
            """)
    else:
        cursor = await db.execute("""
            SELECT cm.*, u.full_name as sender_name, u.phone as sender_phone
            FROM chat_messages cm LEFT JOIN users u ON cm.sender_id = u.id
            WHERE (cm.sender_id = ? AND cm.is_from_admin = 0)
               OR (cm.receiver_id = ? AND cm.is_from_admin = 1)
            ORDER BY cm.created_at ASC
        """, (user["id"], user["id"]))
    msgs = await cursor.fetchall()
    # Marquer comme lu
    if user["role"] != "admin":
        await db.execute("UPDATE chat_messages SET is_read = 1 WHERE receiver_id = ?", (user["id"],))
        await db.commit()
    return [dict(m) for m in msgs]

@app.get("/api/chat/conversations", tags=["Chat"])
async def get_conversations(user=Depends(get_admin_user), db: aiosqlite.Connection = Depends(get_db)):
    """Liste des clients qui ont envoyé des messages"""
    cursor = await db.execute("""
        SELECT DISTINCT u.id, u.phone, u.full_name,
               COUNT(CASE WHEN cm.is_read = 0 AND cm.is_from_admin = 0 THEN 1 END) as unread,
               MAX(cm.created_at) as last_message_at
        FROM chat_messages cm
        JOIN users u ON cm.sender_id = u.id
        WHERE cm.is_from_admin = 0
        GROUP BY u.id ORDER BY last_message_at DESC
    """)
    return [dict(c) for c in await cursor.fetchall()]

# ─── Stats publiques ──────────────────────────────────────────────────────────
@app.get("/api/stats/public", tags=["Stats"])
async def public_stats(db: aiosqlite.Connection = Depends(get_db)):
    prod_cursor = await db.execute("SELECT COUNT(*) FROM products WHERE is_active = 1")
    prod_count = (await prod_cursor.fetchone())[0]
    user_cursor = await db.execute("SELECT COUNT(*) FROM users WHERE role = 'client'")
    user_count = (await user_cursor.fetchone())[0]
    return {"products": prod_count, "clients": user_count}

# ─── WebSocket ────────────────────────────────────────────────────────────────
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket, token: Optional[str] = None, is_admin: Optional[str] = None):
    user_id = None
    admin = is_admin == "true"
    if token:
        try:
            payload = decode_token(token)
            user_id = payload.get("user_id")
            if payload.get("role") == "admin":
                admin = True
        except Exception:
            pass
    await manager.connect(websocket, user_id=user_id, is_admin=admin)
    try:
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        manager.disconnect(websocket, user_id=user_id, is_admin=admin)

# ─── Pages HTML ───────────────────────────────────────────────────────────────
@app.get("/admin")
async def admin_panel():
    if os.path.exists("admin.html"):
        return FileResponse("admin.html")
    raise HTTPException(status_code=404, detail="admin.html introuvable")

@app.get("/")
async def accueil():
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    return {"message": "Digital Fashion Store API v2.0", "status": "online"}

@app.get("/api/health")
async def health_check():
    return {"status": "🟢 API en ligne", "version": "2.0.0"}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=3000, log_level="info")
