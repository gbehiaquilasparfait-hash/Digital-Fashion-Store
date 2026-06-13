"""
Digital Fashion Store - Backend FastAPI v3.0
Conformément au cahier des charges v3.0 :
- Suppression complète de Wave
- Système de panier intelligent
- Commandes avec QR Code unique + numéro de commande
- Suivi des commandes (5 statuts)
- Attribution des commandes par admin à un livreur
- Version marchand : description magasin obligatoire, scan QR
- Version livreur : voir commandes assignées, scanner QR
- GPS / géolocalisation
- Notifications système en temps réel
- Historique complet
- Sécurité QR : seul le livreur assigné peut scanner
"""

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException, Depends, status, UploadFile, File, Form, WebSocket, WebSocketDisconnect, Query
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
import qrcode
import io
import base64
import aiofiles
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
DELETE_CODE = os.getenv("DELETE_CODE", "Q17585644q")
SUSPEND_CODE = os.getenv("SUSPEND_CODE", "Q17585644q")

# Statuts des commandes
ORDER_STATUS = {
    "validee": "Commande validée",
    "attente_livreur": "En attente d'attribution d'un livreur",
    "en_livraison": "Commande en cours de livraison",
    "livree": "Commande livrée",
    "paiement_recu": "Paiement reçu",
    "annulee": "Commande annulée",
}

@asynccontextmanager
async def lifespan(app: FastAPI):
    """Gestionnaire de cycle de vie de l'application (remplace @on_event)"""
    await init_db()
    asyncio.create_task(refresh_temp_codes())
    yield

app = FastAPI(title="Digital Fashion Store API", version="3.0.0", lifespan=lifespan)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

os.makedirs("uploads/products", exist_ok=True)
os.makedirs("uploads/kyc", exist_ok=True)
os.makedirs("uploads/qrcodes", exist_ok=True)
app.mount("/uploads", StaticFiles(directory="uploads"), name="uploads")
security = HTTPBearer(auto_error=False)

# ─── WebSocket Manager ────────────────────────────────────────────────────────
class ConnectionManager:
    def __init__(self):
        self.active_connections: dict = {}
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
        all_ws = self.admin_connections[:]
        for wsList in self.active_connections.values():
            all_ws.extend(wsList)
        dead = []
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

def generate_temp_code():
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))

def generate_order_number():
    """Génère un numéro de commande unique : DFS-YYYYMMDD-XXXXXX"""
    date_str = datetime.utcnow().strftime("%Y%m%d")
    random_part = ''.join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"DFS-{date_str}-{random_part}"

def generate_qr_code_data(order_id: str, order_number: str, qr_type: str) -> str:
    """Génère les données du QR code (URL encodée)"""
    return f"DFS-QR:{qr_type}:{order_id}:{order_number}"

# ─── Base de données ──────────────────────────────────────────────────────────
async def get_db():
    async with aiosqlite.connect(DATABASE_FILE) as db:
        db.row_factory = aiosqlite.Row
        yield db

async def init_db():
    try:
        async with aiosqlite.connect(DATABASE_FILE) as db:
            db.row_factory = aiosqlite.Row

            # ── Users
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
                    is_active BOOLEAN DEFAULT 1,
                    suspended BOOLEAN DEFAULT 0,
                    suspended_at TIMESTAMP,
                    kyc_id_front TEXT,
                    kyc_id_back TEXT,
                    kyc_selfie TEXT,
                    kyc_status TEXT DEFAULT 'none',
                    store_description TEXT,
                    gps_consent BOOLEAN DEFAULT 0,
                    last_lat REAL,
                    last_lng REAL,
                    last_gps_at TIMESTAMP
                )
            """)

            # Migration colonnes users
            for col, definition in [
                ("suspended", "BOOLEAN DEFAULT 0"),
                ("suspended_at", "TIMESTAMP"),
                ("kyc_id_front", "TEXT"),
                ("kyc_id_back", "TEXT"),
                ("kyc_selfie", "TEXT"),
                ("kyc_status", "TEXT DEFAULT 'none'"),
                ("store_description", "TEXT"),
                ("gps_consent", "BOOLEAN DEFAULT 0"),
                ("last_lat", "REAL"),
                ("last_lng", "REAL"),
                ("last_gps_at", "TIMESTAMP"),
            ]:
                try:
                    await db.execute(f"ALTER TABLE users ADD COLUMN {col} {definition}")
                except Exception:
                    pass

            # ── Categories
            await db.execute("""
                CREATE TABLE IF NOT EXISTS categories (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    name TEXT NOT NULL,
                    description TEXT,
                    icon TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ── Products
            await db.execute("""
                CREATE TABLE IF NOT EXISTS products (
                    id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    price REAL NOT NULL,
                    stock INTEGER DEFAULT 0,
                    category_id INTEGER REFERENCES categories(id),
                    views INTEGER DEFAULT 0,
                    likes INTEGER DEFAULT 0,
                    is_active BOOLEAN DEFAULT 1,
                    seller_id TEXT REFERENCES users(id),
                    wave_link TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Migration products
            for col, definition in [
                ("seller_id", "TEXT"),
                ("wave_link", "TEXT"),
            ]:
                try:
                    await db.execute(f"ALTER TABLE products ADD COLUMN {col} {definition}")
                except Exception:
                    pass

            # ── Product images
            await db.execute("""
                CREATE TABLE IF NOT EXISTS product_images (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id TEXT REFERENCES products(id) ON DELETE CASCADE,
                    image_path TEXT NOT NULL,
                    is_main BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ── Product likes
            await db.execute("""
                CREATE TABLE IF NOT EXISTS product_likes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT REFERENCES users(id),
                    product_id TEXT REFERENCES products(id),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, product_id)
                )
            """)

            # ── Cart items
            await db.execute("""
                CREATE TABLE IF NOT EXISTS cart_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT REFERENCES users(id),
                    product_id TEXT REFERENCES products(id),
                    quantity INTEGER DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(user_id, product_id)
                )
            """)

            # ── Orders (améliorées)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS orders (
                    id TEXT PRIMARY KEY,
                    order_number TEXT UNIQUE NOT NULL,
                    user_id TEXT REFERENCES users(id),
                    seller_id TEXT REFERENCES users(id),
                    total_price REAL NOT NULL,
                    status TEXT DEFAULT 'validee',
                    client_address TEXT,
                    client_quartier TEXT,
                    client_ville TEXT,
                    client_repere TEXT,
                    client_phone TEXT,
                    qr_code_data TEXT,
                    qr_used BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Migration orders
            for col, definition in [
                ("order_number", "TEXT"),
                ("seller_id", "TEXT"),
                ("client_address", "TEXT"),
                ("client_quartier", "TEXT"),
                ("client_ville", "TEXT"),
                ("client_repere", "TEXT"),
                ("client_phone", "TEXT"),
                ("qr_code_data", "TEXT"),
                ("qr_used", "BOOLEAN DEFAULT 0"),
            ]:
                try:
                    await db.execute(f"ALTER TABLE orders ADD COLUMN {col} {definition}")
                except Exception:
                    pass

            # ── Order items
            await db.execute("""
                CREATE TABLE IF NOT EXISTS order_items (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT REFERENCES orders(id) ON DELETE CASCADE,
                    product_id TEXT REFERENCES products(id),
                    quantity INTEGER DEFAULT 1,
                    unit_price REAL NOT NULL,
                    product_name TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ── Order attributions (admin assigne un livreur)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS order_attributions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT REFERENCES orders(id),
                    admin_id TEXT REFERENCES users(id),
                    livreur_id TEXT REFERENCES users(id),
                    merchant_id TEXT REFERENCES users(id),
                    client_id TEXT REFERENCES users(id),
                    attributed_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(order_id)
                )
            """)

            # ── QR Scans (journal sécurisé)
            await db.execute("""
                CREATE TABLE IF NOT EXISTS qr_scans (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    order_id TEXT REFERENCES orders(id),
                    scanner_id TEXT REFERENCES users(id),
                    scan_type TEXT,
                    is_authorized BOOLEAN DEFAULT 0,
                    lat REAL,
                    lng REAL,
                    scanned_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ── GPS Logs
            await db.execute("""
                CREATE TABLE IF NOT EXISTS gps_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    user_id TEXT REFERENCES users(id),
                    lat REAL,
                    lng REAL,
                    context TEXT,
                    logged_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ── Notifications
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

            # ── Chat messages
            await db.execute("""
                CREATE TABLE IF NOT EXISTS chat_messages (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    sender_id TEXT NOT NULL,
                    receiver_id TEXT,
                    message TEXT NOT NULL,
                    is_from_admin BOOLEAN DEFAULT 0,
                    sender_role TEXT DEFAULT 'client',
                    is_read BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # Migration chat_messages
            for col, definition in [
                ("sender_role", "TEXT DEFAULT 'client'"),
            ]:
                try:
                    await db.execute(f"ALTER TABLE chat_messages ADD COLUMN {col} {definition}")
                except Exception:
                    pass

            # ── Product comments
            await db.execute("""
                CREATE TABLE IF NOT EXISTS product_comments (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_id TEXT REFERENCES products(id) ON DELETE CASCADE,
                    user_id TEXT REFERENCES users(id),
                    message TEXT NOT NULL,
                    parent_id INTEGER REFERENCES product_comments(id),
                    is_from_seller BOOLEAN DEFAULT 0,
                    is_from_admin BOOLEAN DEFAULT 0,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # ── Catégories de base
            base_cats = [
                ('Robes', 'Robes élégantes et tendance', '👗'),
                ('Chaussures', 'Chaussures pour toutes occasions', '👠'),
                ('Sacs', 'Sacs et maroquinerie', '👜'),
                ('Accessoires', 'Bijoux et accessoires mode', '💍'),
                ('Vêtements Homme', 'Mode masculine contemporaine', '👔'),
                ('Sport & Casual', 'Tenues décontractées et sportswear', '👟'),
                ('Téléphones', 'Smartphones et accessoires', '📱'),
                ('Ordinateurs', 'PC, laptops et accessoires', '💻'),
                ('TV & Multimédia', 'Télévisions, radios, audio', '📺'),
                ('Électroménager', 'Frigos, ventilateurs, appareils maison', '🏠'),
                ('Électronique', 'Tous appareils électroniques', '⚡'),
            ]
            for name, desc, icon in base_cats:
                await db.execute(
                    "INSERT OR IGNORE INTO categories (name, description, icon) VALUES (?, ?, ?)",
                    (name, desc, icon)
                )

            # ── Admin par défaut
            cursor = await db.execute("SELECT id FROM users WHERE phone = '00000000'")
            if not await cursor.fetchone():
                admin_id = str(uuid.uuid4())
                admin_pass = bcrypt.hashpw("Admin@1234".encode(), bcrypt.gensalt()).decode()
                await db.execute("""
                    INSERT INTO users (id, phone, password_hash, full_name, role)
                    VALUES (?, '00000000', ?, 'Administrateur', 'admin')
                """, (admin_id, admin_pass))

            await db.commit()
            print("✅ Base de données v3 initialisée")
    except Exception as e:
        print(f"❌ Erreur DB: {e}")
        import traceback; traceback.print_exc()

async def refresh_temp_codes():
    while True:
        await asyncio.sleep(300)
        try:
            async with aiosqlite.connect(DATABASE_FILE) as db:
                db.row_factory = aiosqlite.Row
                cursor = await db.execute(
                    "SELECT id FROM users WHERE role IN ('client','merchant','livreur') AND is_active = 1"
                )
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
    is_active: Optional[bool] = None
    wave_link: Optional[str] = None

class ProductDelete(BaseModel):
    delete_code: str

class CartAdd(BaseModel):
    product_id: str
    quantity: int = 1

class CartUpdate(BaseModel):
    quantity: int

class OrderValidate(BaseModel):
    address: str
    quartier: str
    ville: str
    repere: str
    phone: str

class AttributionCreate(BaseModel):
    order_id: str
    livreur_id: str

class QRScan(BaseModel):
    qr_data: str
    lat: Optional[float] = None
    lng: Optional[float] = None

class PasswordResetVerify(BaseModel):
    phone: str
    temp_code: str
    new_password: str

class ChatMessage(BaseModel):
    message: str
    receiver_id: Optional[str] = None

class CommentCreate(BaseModel):
    message: str
    parent_id: Optional[int] = None

class SuspendAction(BaseModel):
    code: str
    user_id: str
    action: str  # 'suspend' ou 'activate'

class StoreDescriptionUpdate(BaseModel):
    description: str

class GPSUpdate(BaseModel):
    lat: float
    lng: float
    consent: bool = True
    context: Optional[str] = None

class MerchantRegisterData(BaseModel):
    phone: str
    password: str
    full_name: str
    birth_date: str
    store_description: str

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
    u = dict(user)
    if u.get("suspended"):
        raise HTTPException(status_code=403, detail="ACCOUNT_SUSPENDED")
    return u

async def get_current_user_optional(
    credentials: HTTPAuthorizationCredentials = Depends(security),
    db: aiosqlite.Connection = Depends(get_db)
):
    if not credentials:
        return None
    try:
        payload = decode_token(credentials.credentials)
        cursor = await db.execute("SELECT * FROM users WHERE id = ? AND is_active = 1", (payload["user_id"],))
        user = await cursor.fetchone()
        return dict(user) if user else None
    except Exception:
        return None

async def get_admin_user(user=Depends(get_current_user)):
    if user["role"] != "admin":
        raise HTTPException(status_code=403, detail="Accès administrateur requis")
    return user

async def get_merchant_or_admin(user=Depends(get_current_user)):
    if user["role"] not in ("admin", "merchant"):
        raise HTTPException(status_code=403, detail="Accès marchand ou admin requis")
    return user

async def get_livreur_user(user=Depends(get_current_user)):
    if user["role"] != "livreur":
        raise HTTPException(status_code=403, detail="Accès livreur requis")
    return user

async def notify_user(db, user_id: str, title: str, message: str, notif_type: str = "info"):
    """Crée une notification en base et l'envoie via WebSocket"""
    await db.execute("""
        INSERT INTO notifications (user_id, title, message, type)
        VALUES (?, ?, ?, ?)
    """, (user_id, title, message, notif_type))
    await db.commit()
    await manager.send_to_user(user_id, {
        "event": "notification",
        "data": {"title": title, "message": message, "type": notif_type}
    })

# ─── AUTH ─────────────────────────────────────────────────────────────────────
@app.post("/api/auth/register", tags=["Auth"])
async def register(data: UserRegister, db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("SELECT id, is_active FROM users WHERE phone = ?", (data.phone,))
    existing = await cursor.fetchone()
    if existing:
        if not existing["is_active"]:
            new_hash = bcrypt.hashpw(data.password.encode(), bcrypt.gensalt()).decode()
            temp_code = generate_temp_code()
            await db.execute("""
                UPDATE users SET password_hash=?, full_name=?, birth_date=?,
                temp_code=?, temp_code_generated_at=?, is_active=1, suspended=0
                WHERE phone=?
            """, (new_hash, data.full_name, data.birth_date, temp_code,
                  datetime.utcnow().isoformat(), data.phone))
            await db.commit()
            cursor2 = await db.execute("SELECT * FROM users WHERE phone=?", (data.phone,))
            user_row = dict(await cursor2.fetchone())
            token = create_token(user_row["id"], data.phone, user_row["role"])
            return {"access_token": token, "token_type": "bearer",
                    "user": {"id": user_row["id"], "phone": data.phone, "full_name": data.full_name, "role": user_row["role"]}}
        raise HTTPException(status_code=400, detail="Numéro déjà enregistré")

    user_id = str(uuid.uuid4())
    hashed = bcrypt.hashpw(data.password.encode(), bcrypt.gensalt()).decode()
    temp_code = generate_temp_code()

    await db.execute("""
        INSERT INTO users (id, phone, password_hash, full_name, birth_date, temp_code, temp_code_generated_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, data.phone, hashed, data.full_name, data.birth_date,
          temp_code, datetime.utcnow().isoformat()))
    await db.commit()

    cursor = await db.execute("SELECT COUNT(*) FROM users WHERE role='client' AND is_active=1")
    total_clients = (await cursor.fetchone())[0]

    await manager.send_to_admins({
        "event": "new_user",
        "data": {
            "id": user_id, "phone": data.phone, "full_name": data.full_name,
            "birth_date": data.birth_date, "temp_code": temp_code, "role": "client",
            "created_at": datetime.utcnow().isoformat(), "total_clients": total_clients
        }
    })
    await manager.broadcast({"event": "client_count_updated", "data": {"total": total_clients}})

    token = create_token(user_id, data.phone, "client")
    return {"access_token": token, "token_type": "bearer",
            "user": {"id": user_id, "phone": data.phone, "full_name": data.full_name, "role": "client"}}

@app.post("/api/auth/register-merchant", tags=["Auth"])
async def register_merchant(
    phone: str = Form(...),
    password: str = Form(...),
    full_name: str = Form(...),
    birth_date: str = Form(...),
    store_description: str = Form(...),
    id_front: UploadFile = File(...),
    id_back: UploadFile = File(...),
    selfie: UploadFile = File(...),
    db: aiosqlite.Connection = Depends(get_db)
):
    # Validation description magasin (min 4 mots, max 100 mots)
    words = store_description.strip().split()
    if len(words) < 4:
        raise HTTPException(status_code=400, detail="La description du magasin doit avoir au moins 4 mots")
    if len(words) > 100:
        raise HTTPException(status_code=400, detail="La description du magasin ne doit pas dépasser 100 mots")

    cursor = await db.execute("SELECT id FROM users WHERE phone = ?", (phone,))
    if await cursor.fetchone():
        raise HTTPException(status_code=400, detail="Numéro déjà enregistré")

    user_id = str(uuid.uuid4())
    hashed = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    temp_code = generate_temp_code()

    kyc_paths = {}
    for fname, fobj in [("front", id_front), ("back", id_back), ("selfie", selfie)]:
        ext = fobj.filename.split(".")[-1] if "." in fobj.filename else "jpg"
        filename = f"{user_id}_{fname}.{ext}"
        filepath = f"uploads/kyc/{filename}"
        async with aiofiles.open(filepath, "wb") as f:
            await f.write(await fobj.read())
        kyc_paths[fname] = filepath

    await db.execute("""
        INSERT INTO users (id, phone, password_hash, full_name, birth_date,
            temp_code, temp_code_generated_at, role, kyc_id_front, kyc_id_back,
            kyc_selfie, kyc_status, store_description)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'merchant', ?, ?, ?, 'pending', ?)
    """, (user_id, phone, hashed, full_name, birth_date,
          temp_code, datetime.utcnow().isoformat(),
          kyc_paths["front"], kyc_paths["back"], kyc_paths["selfie"],
          store_description))
    await db.commit()

    await manager.send_to_admins({
        "event": "new_merchant",
        "data": {
            "id": user_id, "phone": phone, "full_name": full_name,
            "store_description": store_description,
            "kyc_front": kyc_paths["front"], "kyc_back": kyc_paths["back"],
            "kyc_selfie": kyc_paths["selfie"],
            "created_at": datetime.utcnow().isoformat()
        }
    })

    token = create_token(user_id, phone, "merchant")
    return {"access_token": token, "token_type": "bearer",
            "user": {"id": user_id, "phone": phone, "full_name": full_name, "role": "merchant",
                     "store_description": store_description}}

@app.post("/api/auth/register-livreur", tags=["Auth"])
async def register_livreur(data: UserRegister, db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("SELECT id FROM users WHERE phone = ?", (data.phone,))
    if await cursor.fetchone():
        raise HTTPException(status_code=400, detail="Numéro déjà enregistré")

    user_id = str(uuid.uuid4())
    hashed = bcrypt.hashpw(data.password.encode(), bcrypt.gensalt()).decode()
    temp_code = generate_temp_code()

    await db.execute("""
        INSERT INTO users (id, phone, password_hash, full_name, birth_date,
            temp_code, temp_code_generated_at, role)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'livreur')
    """, (user_id, data.phone, hashed, data.full_name, data.birth_date,
          temp_code, datetime.utcnow().isoformat()))
    await db.commit()

    await manager.send_to_admins({
        "event": "new_livreur",
        "data": {"id": user_id, "phone": data.phone, "full_name": data.full_name,
                 "created_at": datetime.utcnow().isoformat()}
    })

    token = create_token(user_id, data.phone, "livreur")
    return {"access_token": token, "token_type": "bearer",
            "user": {"id": user_id, "phone": data.phone, "full_name": data.full_name, "role": "livreur"}}

@app.post("/api/auth/login", tags=["Auth"])
async def login(data: UserLogin, db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("SELECT * FROM users WHERE phone = ? AND is_active = 1", (data.phone,))
    user = await cursor.fetchone()
    if not user:
        raise HTTPException(status_code=401, detail="Identifiants invalides")
    user_dict = dict(user)
    if not bcrypt.checkpw(data.password.encode(), user_dict["password_hash"].encode()):
        raise HTTPException(status_code=401, detail="Identifiants invalides")
    if user_dict.get("suspended"):
        raise HTTPException(status_code=403, detail="ACCOUNT_SUSPENDED")
    token = create_token(user_dict["id"], user_dict["phone"], user_dict["role"])
    return {
        "access_token": token, "token_type": "bearer",
        "user": {
            "id": user_dict["id"], "phone": user_dict["phone"],
            "full_name": user_dict["full_name"], "role": user_dict["role"],
            "store_description": user_dict.get("store_description"),
            "kyc_status": user_dict.get("kyc_status")
        }
    }

@app.post("/api/auth/reset-password", tags=["Auth"])
async def reset_password(data: PasswordResetVerify, db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("SELECT * FROM users WHERE phone = ? AND is_active = 1", (data.phone,))
    user = await cursor.fetchone()
    if not user:
        raise HTTPException(status_code=404, detail="Utilisateur introuvable")
    user_dict = dict(user)
    if user_dict.get("temp_code") != data.temp_code.upper():
        raise HTTPException(status_code=400, detail="Code temporaire incorrect")
    new_hash = bcrypt.hashpw(data.new_password.encode(), bcrypt.gensalt()).decode()
    new_code = generate_temp_code()
    await db.execute(
        "UPDATE users SET password_hash=?, temp_code=?, temp_code_generated_at=? WHERE phone=?",
        (new_hash, new_code, datetime.utcnow().isoformat(), data.phone)
    )
    await db.commit()
    return {"message": "Mot de passe changé avec succès"}

@app.get("/api/auth/me", tags=["Auth"])
async def get_me(user=Depends(get_current_user)):
    return {k: v for k, v in user.items() if k not in ("password_hash",)}

# ─── Description du magasin (Marchand) ───────────────────────────────────────
@app.put("/api/merchant/store-description", tags=["Marchand"])
async def update_store_description(
    data: StoreDescriptionUpdate,
    user=Depends(get_merchant_or_admin),
    db: aiosqlite.Connection = Depends(get_db)
):
    words = data.description.strip().split()
    if len(words) < 4:
        raise HTTPException(status_code=400, detail="Description trop courte (minimum 4 mots)")
    if len(words) > 100:
        raise HTTPException(status_code=400, detail="Description trop longue (maximum 100 mots)")
    await db.execute("UPDATE users SET store_description=? WHERE id=?",
                     (data.description, user["id"]))
    await db.commit()
    return {"message": "Description mise à jour", "store_description": data.description}

# ─── GPS ──────────────────────────────────────────────────────────────────────
@app.post("/api/gps/update", tags=["GPS"])
async def update_gps(
    data: GPSUpdate,
    user=Depends(get_current_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    await db.execute("""
        UPDATE users SET last_lat=?, last_lng=?, last_gps_at=?, gps_consent=?
        WHERE id=?
    """, (data.lat, data.lng, datetime.utcnow().isoformat(), 1 if data.consent else 0, user["id"]))
    await db.execute("""
        INSERT INTO gps_logs (user_id, lat, lng, context) VALUES (?, ?, ?, ?)
    """, (user["id"], data.lat, data.lng, data.context or "manual"))
    await db.commit()
    # Notifier l'admin en temps réel si livreur
    if user["role"] == "livreur":
        await manager.send_to_admins({
            "event": "livreur_gps_updated",
            "data": {"livreur_id": user["id"], "name": user["full_name"],
                     "lat": data.lat, "lng": data.lng, "at": datetime.utcnow().isoformat()}
        })
    return {"message": "GPS mis à jour"}

@app.get("/api/admin/gps", tags=["GPS"])
async def get_all_gps(user=Depends(get_admin_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT id, phone, full_name, role, last_lat, last_lng, last_gps_at, gps_consent
        FROM users
        WHERE last_lat IS NOT NULL AND gps_consent=1
        ORDER BY last_gps_at DESC
    """)
    return [dict(u) for u in await cursor.fetchall()]

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
    seller_id: Optional[str] = None,
    db: aiosqlite.Connection = Depends(get_db)
):
    offset = (page - 1) * limit
    where_clauses = ["p.is_active = 1"]
    params = []
    if category_id:
        where_clauses.append("p.category_id = ?")
        params.append(category_id)
    if search:
        where_clauses.append("(p.name LIKE ? OR p.id LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])
    if seller_id:
        where_clauses.append("p.seller_id = ?")
        params.append(seller_id)
    where_sql = " AND ".join(where_clauses)
    sort_map = {
        "recent": "p.created_at DESC", "popular": "p.views DESC",
        "liked": "p.likes DESC", "price_asc": "p.price ASC", "price_desc": "p.price DESC"
    }
    order_sql = sort_map.get(sort, "p.created_at DESC")

    cursor = await db.execute(f"SELECT COUNT(*) FROM products p WHERE {where_sql}", params)
    total = (await cursor.fetchone())[0]

    cursor = await db.execute(
        f"""SELECT p.*, c.name as category_name, u.full_name as seller_name, u.store_description as seller_store
            FROM products p
            LEFT JOIN categories c ON p.category_id = c.id
            LEFT JOIN users u ON p.seller_id = u.id
            WHERE {where_sql} ORDER BY {order_sql} LIMIT ? OFFSET ?""",
        params + [limit, offset]
    )
    products = await cursor.fetchall()
    result = []
    for p in products:
        p_dict = dict(p)
        img_cursor = await db.execute(
            "SELECT image_path, is_main FROM product_images WHERE product_id=? ORDER BY is_main DESC",
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
        SELECT p.*, c.name as category_name, u.full_name as seller_name,
               u.phone as seller_phone, u.store_description as seller_store
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        LEFT JOIN users u ON p.seller_id = u.id
        WHERE p.id = ? AND p.is_active = 1
    """, (product_id,))
    product = await cursor.fetchone()
    if not product:
        raise HTTPException(status_code=404, detail="Produit introuvable")
    p_dict = dict(product)
    await db.execute("UPDATE products SET views = views + 1 WHERE id = ?", (product_id,))
    await db.commit()
    new_views = p_dict["views"] + 1
    await manager.broadcast({"event": "product_stats_updated",
                              "data": {"product_id": product_id, "views": new_views}})
    img_cursor = await db.execute(
        "SELECT image_path, is_main FROM product_images WHERE product_id=? ORDER BY is_main DESC",
        (product_id,)
    )
    images = await img_cursor.fetchall()
    p_dict["images"] = [dict(img) for img in images]
    p_dict["main_image"] = images[0]["image_path"] if images else None
    p_dict["views"] = new_views
    return p_dict

@app.post("/api/products", tags=["Produits"])
async def create_product(
    product: ProductCreate,
    user=Depends(get_merchant_or_admin),
    db: aiosqlite.Connection = Depends(get_db)
):
    product_id = str(uuid.uuid4())
    seller_id = None if user["role"] == "admin" else user["id"]
    await db.execute("""
        INSERT INTO products (id, name, description, price, stock, category_id, seller_id, wave_link)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (product_id, product.name, product.description, product.price,
          product.stock, product.category_id, seller_id, product.wave_link))
    await db.commit()
    cursor = await db.execute("""
        SELECT p.*, c.name as category_name FROM products p
        LEFT JOIN categories c ON p.category_id = c.id WHERE p.id = ?
    """, (product_id,))
    new_product = dict(await cursor.fetchone())

    if seller_id:
        await manager.send_to_admins({
            "event": "new_merchant_product",
            "data": {
                "product": new_product,
                "seller_phone": user["phone"],
                "seller_name": user["full_name"],
                "seller_id": seller_id
            }
        })
    await manager.broadcast({"event": "product_added", "data": new_product})
    return new_product

@app.put("/api/products/{product_id}", tags=["Produits"])
async def update_product(
    product_id: str,
    product: ProductUpdate,
    user=Depends(get_merchant_or_admin),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute("SELECT * FROM products WHERE id = ?", (product_id,))
    existing = await cursor.fetchone()
    if not existing:
        raise HTTPException(status_code=404, detail="Produit introuvable")
    existing = dict(existing)
    if user["role"] == "merchant" and existing.get("seller_id") != user["id"]:
        raise HTTPException(status_code=403, detail="Vous ne pouvez modifier que vos propres produits")

    updates = {}
    for field in ["name", "description", "price", "stock", "category_id", "is_active", "wave_link"]:
        val = getattr(product, field)
        if val is not None:
            updates[field] = val
    if updates:
        updates["updated_at"] = datetime.utcnow().isoformat()
        set_clause = ", ".join([f"{k} = ?" for k in updates.keys()])
        await db.execute(f"UPDATE products SET {set_clause} WHERE id = ?",
                         list(updates.values()) + [product_id])
        await db.commit()
    cursor = await db.execute("""
        SELECT p.*, c.name as category_name FROM products p
        LEFT JOIN categories c ON p.category_id = c.id WHERE p.id = ?
    """, (product_id,))
    return dict(await cursor.fetchone())

@app.delete("/api/products/{product_id}", tags=["Produits"])
async def delete_product(
    product_id: str,
    body: ProductDelete,
    user=Depends(get_merchant_or_admin),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute("SELECT * FROM products WHERE id = ?", (product_id,))
    existing = await cursor.fetchone()
    if not existing:
        raise HTTPException(status_code=404, detail="Produit introuvable")
    existing = dict(existing)
    if user["role"] == "merchant" and existing.get("seller_id") != user["id"]:
        raise HTTPException(status_code=403, detail="Accès refusé")
    if body.delete_code != DELETE_CODE:
        raise HTTPException(status_code=403, detail="Code de suppression incorrect")
    await db.execute("UPDATE products SET is_active = 0 WHERE id = ?", (product_id,))
    await db.commit()
    await manager.broadcast({"event": "product_deleted", "data": {"product_id": product_id}})
    return {"message": "Produit supprimé avec succès"}

@app.post("/api/products/{product_id}/images", tags=["Produits"])
async def upload_product_images(
    product_id: str,
    files: List[UploadFile] = File(...),
    user=Depends(get_merchant_or_admin),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute("SELECT * FROM products WHERE id = ?", (product_id,))
    existing = await cursor.fetchone()
    if not existing:
        raise HTTPException(status_code=404, detail="Produit introuvable")
    existing = dict(existing)
    if user["role"] == "merchant" and existing.get("seller_id") != user["id"]:
        raise HTTPException(status_code=403, detail="Accès refusé")

    uploaded = []
    for i, file in enumerate(files):
        if not file.content_type.startswith("image/"):
            continue
        ext = file.filename.split(".")[-1] if "." in file.filename else "jpg"
        filename = f"{product_id}_{uuid.uuid4().hex[:8]}.{ext}"
        filepath = f"uploads/products/{filename}"
        async with aiofiles.open(filepath, "wb") as f:
            await f.write(await file.read())
        is_main = 1 if i == 0 else 0
        await db.execute(
            "INSERT INTO product_images (product_id, image_path, is_main) VALUES (?, ?, ?)",
            (product_id, filepath, is_main)
        )
        uploaded.append(filepath)
    await db.commit()
    return {"uploaded": uploaded, "count": len(uploaded)}

# ─── Likes ────────────────────────────────────────────────────────────────────
@app.post("/api/products/{product_id}/like", tags=["Likes"])
async def like_product(product_id: str, user=Depends(get_current_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute(
        "SELECT id FROM product_likes WHERE user_id = ? AND product_id = ?",
        (user["id"], product_id)
    )
    existing = await cursor.fetchone()
    if existing:
        await db.execute("DELETE FROM product_likes WHERE user_id = ? AND product_id = ?",
                         (user["id"], product_id))
        await db.execute("UPDATE products SET likes = MAX(0, likes - 1) WHERE id = ?", (product_id,))
        liked = False
    else:
        await db.execute("INSERT INTO product_likes (user_id, product_id) VALUES (?, ?)",
                         (user["id"], product_id))
        await db.execute("UPDATE products SET likes = likes + 1 WHERE id = ?", (product_id,))
        liked = True
    await db.commit()
    cursor = await db.execute("SELECT likes, views FROM products WHERE id = ?", (product_id,))
    row = dict(await cursor.fetchone())
    await manager.broadcast({"event": "product_liked", "data": {"product_id": product_id, "likes": row["likes"]}})
    return {"liked": liked, "likes": row["likes"]}

# ─── Commentaires ─────────────────────────────────────────────────────────────
@app.get("/api/products/{product_id}/comments", tags=["Commentaires"])
async def get_comments(product_id: str, db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT pc.*, u.full_name as author_name
        FROM product_comments pc
        LEFT JOIN users u ON pc.user_id = u.id
        WHERE pc.product_id = ? AND pc.parent_id IS NULL
        ORDER BY pc.created_at ASC
    """, (product_id,))
    comments = [dict(c) for c in await cursor.fetchall()]
    for c in comments:
        rep_cursor = await db.execute("""
            SELECT pc.*, u.full_name as author_name
            FROM product_comments pc
            LEFT JOIN users u ON pc.user_id = u.id
            WHERE pc.parent_id = ? ORDER BY pc.created_at ASC
        """, (c["id"],))
        c["replies"] = [dict(r) for r in await rep_cursor.fetchall()]
    return comments

@app.post("/api/products/{product_id}/comments", tags=["Commentaires"])
async def add_comment(
    product_id: str,
    data: CommentCreate,
    user=Depends(get_current_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute("SELECT * FROM products WHERE id = ? AND is_active = 1", (product_id,))
    product = await cursor.fetchone()
    if not product:
        raise HTTPException(status_code=404, detail="Produit introuvable")
    product_dict = dict(product)
    is_from_seller = user["role"] == "merchant" and product_dict.get("seller_id") == user["id"]
    is_from_admin = user["role"] == "admin"
    await db.execute("""
        INSERT INTO product_comments (product_id, user_id, message, parent_id, is_from_seller, is_from_admin)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (product_id, user["id"], data.message, data.parent_id,
          1 if is_from_seller else 0, 1 if is_from_admin else 0))
    await db.commit()
    payload = {
        "event": "new_comment",
        "data": {
            "product_id": product_id, "product_name": product_dict["name"],
            "author": user["full_name"], "message": data.message,
            "is_from_seller": is_from_seller, "is_from_admin": is_from_admin
        }
    }
    await manager.send_to_admins(payload)
    if not is_from_seller and not is_from_admin and product_dict.get("seller_id"):
        await manager.send_to_user(product_dict["seller_id"], payload)
    return {"message": "Commentaire ajouté"}

# ─── PANIER ───────────────────────────────────────────────────────────────────
@app.get("/api/cart", tags=["Panier"])
async def get_cart(user=Depends(get_current_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT ci.id, ci.quantity, ci.product_id,
               p.name, p.price, p.stock, p.is_active,
               c.name as category_name
        FROM cart_items ci
        JOIN products p ON ci.product_id = p.id
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE ci.user_id = ?
        ORDER BY ci.created_at DESC
    """, (user["id"],))
    items = []
    for row in await cursor.fetchall():
        item = dict(row)
        img_cursor = await db.execute(
            "SELECT image_path FROM product_images WHERE product_id=? ORDER BY is_main DESC LIMIT 1",
            (item["product_id"],)
        )
        img = await img_cursor.fetchone()
        item["main_image"] = img["image_path"] if img else None
        item["subtotal"] = item["price"] * item["quantity"]
        items.append(item)
    total = sum(i["subtotal"] for i in items)
    return {"items": items, "total": total, "count": len(items)}

@app.post("/api/cart", tags=["Panier"])
async def add_to_cart(
    data: CartAdd,
    user=Depends(get_current_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute("SELECT * FROM products WHERE id=? AND is_active=1", (data.product_id,))
    product = await cursor.fetchone()
    if not product:
        raise HTTPException(status_code=404, detail="Produit introuvable")
    product_dict = dict(product)
    if product_dict["stock"] < data.quantity:
        raise HTTPException(status_code=400, detail="Stock insuffisant")

    # Upsert cart item
    cursor = await db.execute(
        "SELECT id, quantity FROM cart_items WHERE user_id=? AND product_id=?",
        (user["id"], data.product_id)
    )
    existing = await cursor.fetchone()
    if existing:
        new_qty = existing["quantity"] + data.quantity
        if product_dict["stock"] < new_qty:
            raise HTTPException(status_code=400, detail="Stock insuffisant pour cette quantité")
        await db.execute(
            "UPDATE cart_items SET quantity=? WHERE id=?",
            (new_qty, existing["id"])
        )
    else:
        await db.execute(
            "INSERT INTO cart_items (user_id, product_id, quantity) VALUES (?, ?, ?)",
            (user["id"], data.product_id, data.quantity)
        )
    await db.commit()
    return {"message": "Produit ajouté au panier"}

@app.put("/api/cart/{item_id}", tags=["Panier"])
async def update_cart_item(
    item_id: int,
    data: CartUpdate,
    user=Depends(get_current_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute(
        "SELECT ci.*, p.stock FROM cart_items ci JOIN products p ON ci.product_id=p.id WHERE ci.id=? AND ci.user_id=?",
        (item_id, user["id"])
    )
    item = await cursor.fetchone()
    if not item:
        raise HTTPException(status_code=404, detail="Article introuvable")
    if data.quantity <= 0:
        await db.execute("DELETE FROM cart_items WHERE id=?", (item_id,))
    else:
        if item["stock"] < data.quantity:
            raise HTTPException(status_code=400, detail="Stock insuffisant")
        await db.execute("UPDATE cart_items SET quantity=? WHERE id=?", (data.quantity, item_id))
    await db.commit()
    return {"message": "Panier mis à jour"}

@app.delete("/api/cart/{item_id}", tags=["Panier"])
async def remove_from_cart(
    item_id: int,
    user=Depends(get_current_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute(
        "SELECT id FROM cart_items WHERE id=? AND user_id=?", (item_id, user["id"])
    )
    if not await cursor.fetchone():
        raise HTTPException(status_code=404, detail="Article introuvable")
    await db.execute("DELETE FROM cart_items WHERE id=?", (item_id,))
    await db.commit()
    return {"message": "Article supprimé du panier"}

@app.delete("/api/cart", tags=["Panier"])
async def clear_cart(user=Depends(get_current_user), db: aiosqlite.Connection = Depends(get_db)):
    await db.execute("DELETE FROM cart_items WHERE user_id=?", (user["id"],))
    await db.commit()
    return {"message": "Panier vidé"}

# ─── COMMANDES ────────────────────────────────────────────────────────────────
@app.post("/api/orders/validate", tags=["Commandes"])
async def validate_order(
    data: OrderValidate,
    user=Depends(get_current_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    """Valide le panier et crée une commande avec QR code"""
    # Récupérer le panier
    cursor = await db.execute("""
        SELECT ci.*, p.name as product_name, p.price, p.stock, p.seller_id
        FROM cart_items ci
        JOIN products p ON ci.product_id = p.id
        WHERE ci.user_id=? AND p.is_active=1
    """, (user["id"],))
    items = [dict(i) for i in await cursor.fetchall()]

    if not items:
        raise HTTPException(status_code=400, detail="Votre panier est vide")

    # Vérifier les stocks
    for item in items:
        if item["stock"] < item["quantity"]:
            raise HTTPException(
                status_code=400,
                detail=f"Stock insuffisant pour {item['product_name']}"
            )

    # Calculer le total et grouper par vendeur
    total_price = sum(i["price"] * i["quantity"] for i in items)

    # Créer la commande
    order_id = str(uuid.uuid4())
    order_number = generate_order_number()
    qr_data = generate_qr_code_data(order_id, order_number, "ORDER")

    # Déterminer le seller_id (premier vendeur, ou None si admin)
    seller_id = items[0].get("seller_id") if items else None

    await db.execute("""
        INSERT INTO orders (id, order_number, user_id, seller_id, total_price, status,
            client_address, client_quartier, client_ville, client_repere, client_phone, qr_code_data)
        VALUES (?, ?, ?, ?, ?, 'validee', ?, ?, ?, ?, ?, ?)
    """, (order_id, order_number, user["id"], seller_id, total_price,
          data.address, data.quartier, data.ville, data.repere, data.phone, qr_data))

    # Créer les order_items et décrémenter les stocks
    for item in items:
        await db.execute("""
            INSERT INTO order_items (order_id, product_id, quantity, unit_price, product_name)
            VALUES (?, ?, ?, ?, ?)
        """, (order_id, item["product_id"], item["quantity"], item["price"], item["product_name"]))
        await db.execute(
            "UPDATE products SET stock = stock - ? WHERE id = ?",
            (item["quantity"], item["product_id"])
        )

    # Vider le panier
    await db.execute("DELETE FROM cart_items WHERE user_id=?", (user["id"],))
    await db.commit()

    # Créer notification pour le client
    await notify_user(db, user["id"],
                      "Commande validée ✅",
                      f"Votre commande #{order_number} a été validée. Un livreur vous sera attribué bientôt.",
                      "order")

    # Notifier admin et marchand
    await manager.send_to_admins({
        "event": "new_order",
        "data": {
            "order_id": order_id, "order_number": order_number,
            "client_name": user["full_name"], "client_phone": user["phone"],
            "total_price": total_price, "items_count": len(items),
            "status": "validee"
        }
    })
    if seller_id:
        await notify_user(db, seller_id,
                          "Nouvelle commande 🛍️",
                          f"Commande #{order_number} reçue pour {total_price:,.0f} FCFA",
                          "order")

    return {
        "order_id": order_id,
        "order_number": order_number,
        "total_price": total_price,
        "status": "validee",
        "qr_code_data": qr_data,
        "created_at": datetime.utcnow().isoformat()
    }

@app.post("/api/orders/{order_id}/cancel", tags=["Commandes"])
async def cancel_order(
    order_id: str,
    user=Depends(get_current_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute(
        "SELECT * FROM orders WHERE id=? AND user_id=?", (order_id, user["id"])
    )
    order = await cursor.fetchone()
    if not order:
        raise HTTPException(status_code=404, detail="Commande introuvable")
    order_dict = dict(order)
    if order_dict["status"] not in ("validee", "attente_livreur"):
        raise HTTPException(status_code=400, detail="Impossible d'annuler une commande en cours de livraison")

    # Remettre les stocks
    items_cursor = await db.execute(
        "SELECT * FROM order_items WHERE order_id=?", (order_id,)
    )
    for item in await items_cursor.fetchall():
        await db.execute(
            "UPDATE products SET stock = stock + ? WHERE id = ?",
            (item["quantity"], item["product_id"])
        )

    await db.execute(
        "UPDATE orders SET status='annulee', updated_at=? WHERE id=?",
        (datetime.utcnow().isoformat(), order_id)
    )
    await db.commit()
    await manager.send_to_admins({
        "event": "order_cancelled",
        "data": {"order_id": order_id, "order_number": order_dict["order_number"]}
    })
    return {"message": "Commande annulée"}

@app.get("/api/orders/my", tags=["Commandes"])
async def get_my_orders(user=Depends(get_current_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT o.*, u.full_name as livreur_name, u.phone as livreur_phone
        FROM orders o
        LEFT JOIN order_attributions oa ON o.id = oa.order_id
        LEFT JOIN users u ON oa.livreur_id = u.id
        WHERE o.user_id=?
        ORDER BY o.created_at DESC
    """, (user["id"],))
    orders = []
    for row in await cursor.fetchall():
        o = dict(row)
        items_cursor = await db.execute(
            "SELECT * FROM order_items WHERE order_id=?", (o["id"],)
        )
        o["items"] = [dict(i) for i in await items_cursor.fetchall()]
        orders.append(o)
    return orders

@app.get("/api/orders/{order_id}", tags=["Commandes"])
async def get_order_detail(
    order_id: str,
    user=Depends(get_current_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute("""
        SELECT o.*, u.full_name as livreur_name, u.phone as livreur_phone
        FROM orders o
        LEFT JOIN order_attributions oa ON o.id = oa.order_id
        LEFT JOIN users u ON oa.livreur_id = u.id
        WHERE o.id=?
    """, (order_id,))
    order = await cursor.fetchone()
    if not order:
        raise HTTPException(status_code=404, detail="Commande introuvable")
    o = dict(order)

    # Vérifier accès : client, marchand concerné, livreur assigné, admin
    if user["role"] == "client" and o["user_id"] != user["id"]:
        raise HTTPException(status_code=403, detail="Accès refusé")
    if user["role"] == "merchant" and o.get("seller_id") != user["id"]:
        raise HTTPException(status_code=403, detail="Accès refusé")

    items_cursor = await db.execute("SELECT * FROM order_items WHERE order_id=?", (order_id,))
    o["items"] = [dict(i) for i in await items_cursor.fetchall()]
    return o

# ─── ADMIN : Attribution des commandes ───────────────────────────────────────
@app.post("/api/admin/orders/attribute", tags=["Admin"])
async def attribute_order(
    data: AttributionCreate,
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    """L'admin attribue une commande à un livreur"""
    cursor = await db.execute("SELECT * FROM orders WHERE id=?", (data.order_id,))
    order = await cursor.fetchone()
    if not order:
        raise HTTPException(status_code=404, detail="Commande introuvable")
    o = dict(order)

    cursor = await db.execute("SELECT * FROM users WHERE id=? AND role='livreur' AND is_active=1", (data.livreur_id,))
    livreur = await cursor.fetchone()
    if not livreur:
        raise HTTPException(status_code=404, detail="Livreur introuvable")
    livreur_dict = dict(livreur)

    # Enregistrer l'attribution
    await db.execute("""
        INSERT OR REPLACE INTO order_attributions
            (order_id, admin_id, livreur_id, merchant_id, client_id, attributed_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (data.order_id, user["id"], data.livreur_id,
          o.get("seller_id"), o["user_id"],
          datetime.utcnow().isoformat()))

    # Mettre à jour le statut
    await db.execute(
        "UPDATE orders SET status='attente_livreur', updated_at=? WHERE id=?",
        (datetime.utcnow().isoformat(), data.order_id)
    )
    await db.commit()

    # Notifications
    await notify_user(db, data.livreur_id,
                      "Nouvelle mission 🚚",
                      f"Commande #{o['order_number']} vous a été attribuée",
                      "mission")
    await notify_user(db, o["user_id"],
                      "Livreur attribué 🚚",
                      f"Un livreur a été assigné à votre commande #{o['order_number']}",
                      "order")
    if o.get("seller_id"):
        await notify_user(db, o["seller_id"],
                          "Commande prise en charge 📦",
                          f"Commande #{o['order_number']} : livreur {livreur_dict['full_name']} assigné",
                          "order")

    return {
        "message": "Commande attribuée avec succès",
        "order_number": o["order_number"],
        "livreur": livreur_dict["full_name"]
    }

# ─── QR CODE : Scan ───────────────────────────────────────────────────────────
@app.post("/api/qr/scan", tags=["QR"])
async def scan_qr(
    data: QRScan,
    user=Depends(get_current_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    """Scan d'un QR code par un livreur"""
    # Parser le QR : DFS-QR:ORDER:order_id:order_number
    parts = data.qr_data.split(":")
    if len(parts) < 3 or parts[0] != "DFS-QR":
        raise HTTPException(status_code=400, detail="QR Code invalide")

    qr_type = parts[1]
    order_id = parts[2]

    # Récupérer la commande
    cursor = await db.execute("SELECT * FROM orders WHERE id=?", (order_id,))
    order = await cursor.fetchone()
    if not order:
        raise HTTPException(status_code=404, detail="Commande introuvable")
    o = dict(order)

    if o.get("qr_used"):
        raise HTTPException(status_code=400, detail="Ce QR Code a déjà été utilisé")

    # Vérifier que le livreur est autorisé
    cursor = await db.execute(
        "SELECT * FROM order_attributions WHERE order_id=? AND livreur_id=?",
        (order_id, user["id"])
    )
    attribution = await cursor.fetchone()
    is_authorized = attribution is not None or user["role"] == "admin"

    # Enregistrer le scan
    await db.execute("""
        INSERT INTO qr_scans (order_id, scanner_id, scan_type, is_authorized, lat, lng)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (order_id, user["id"], qr_type, 1 if is_authorized else 0,
          data.lat, data.lng))
    await db.commit()

    if not is_authorized:
        # Alerter l'admin
        await manager.send_to_admins({
            "event": "unauthorized_qr_scan",
            "data": {
                "order_id": order_id, "order_number": o["order_number"],
                "scanner_id": user["id"], "scanner_name": user["full_name"],
                "scanner_phone": user["phone"],
                "scan_type": qr_type, "at": datetime.utcnow().isoformat()
            }
        })
        raise HTTPException(status_code=403, detail="Scan non autorisé : vous n'êtes pas le livreur assigné à cette commande")

    attr_dict = dict(attribution) if attribution else {}

    if qr_type == "ORDER":
        # Scan chez le marchand → statut "en_livraison"
        await db.execute(
            "UPDATE orders SET status='en_livraison', updated_at=? WHERE id=?",
            (datetime.utcnow().isoformat(), order_id)
        )
        await db.commit()

        # GPS log
        if data.lat and data.lng:
            await db.execute(
                "INSERT INTO gps_logs (user_id, lat, lng, context) VALUES (?, ?, ?, ?)",
                (user["id"], data.lat, data.lng, f"scan_marchand:{order_id}")
            )
            await db.commit()

        new_status = "Commande en cours de livraison"
        # Notifications à tous
        for uid in [o["user_id"], o.get("seller_id"), attr_dict.get("admin_id")]:
            if uid:
                await notify_user(db, uid,
                                  "En cours de livraison 🚚",
                                  f"Commande #{o['order_number']} est en cours de livraison",
                                  "delivery")

        await manager.broadcast({
            "event": "order_status_updated",
            "data": {"order_id": order_id, "order_number": o["order_number"],
                     "status": "en_livraison", "status_label": new_status}
        })
        return {"message": "Scan marchand validé", "new_status": "en_livraison",
                "status_label": new_status, "order_number": o["order_number"]}

    elif qr_type == "DELIVERY":
        # Scan chez le client → statut "livree" puis "paiement_recu"
        await db.execute(
            "UPDATE orders SET status='livree', qr_used=1, updated_at=? WHERE id=?",
            (datetime.utcnow().isoformat(), order_id)
        )
        await db.commit()

        if data.lat and data.lng:
            await db.execute(
                "INSERT INTO gps_logs (user_id, lat, lng, context) VALUES (?, ?, ?, ?)",
                (user["id"], data.lat, data.lng, f"scan_client:{order_id}")
            )
            await db.commit()

        # Notifications
        await notify_user(db, o["user_id"], "Commande livrée ✅",
                          f"Votre commande #{o['order_number']} a été livrée !", "delivery")
        if o.get("seller_id"):
            await notify_user(db, o["seller_id"], "Colis livré 📦",
                              f"Commande #{o['order_number']} livrée au client", "delivery")
        await manager.send_to_admins({
            "event": "order_delivered",
            "data": {"order_id": order_id, "order_number": o["order_number"]}
        })
        await manager.broadcast({
            "event": "order_status_updated",
            "data": {"order_id": order_id, "order_number": o["order_number"],
                     "status": "livree", "status_label": "Commande livrée"}
        })
        return {"message": "Livraison confirmée", "new_status": "livree",
                "status_label": "Commande livrée", "order_number": o["order_number"]}

    raise HTTPException(status_code=400, detail="Type de QR invalide")

@app.post("/api/admin/orders/{order_id}/payment-received", tags=["Admin"])
async def mark_payment_received(
    order_id: str,
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    cursor = await db.execute("SELECT * FROM orders WHERE id=?", (order_id,))
    order = await cursor.fetchone()
    if not order:
        raise HTTPException(status_code=404, detail="Commande introuvable")
    o = dict(order)
    await db.execute(
        "UPDATE orders SET status='paiement_recu', updated_at=? WHERE id=?",
        (datetime.utcnow().isoformat(), order_id)
    )
    await db.commit()
    await notify_user(db, o["user_id"], "Paiement reçu 💰",
                      f"Le paiement de votre commande #{o['order_number']} a été reçu", "payment")
    return {"message": "Paiement enregistré"}

# ─── LIVREUR ──────────────────────────────────────────────────────────────────
@app.get("/api/livreur/orders", tags=["Livreur"])
async def get_livreur_orders(user=Depends(get_livreur_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT o.*, oa.attributed_at,
               uc.full_name as client_name, uc.phone as client_phone,
               um.full_name as merchant_name, um.phone as merchant_phone,
               um.store_description as merchant_store
        FROM orders o
        JOIN order_attributions oa ON o.id = oa.order_id
        LEFT JOIN users uc ON o.user_id = uc.id
        LEFT JOIN users um ON o.seller_id = um.id
        WHERE oa.livreur_id=?
        ORDER BY o.created_at DESC
    """, (user["id"],))
    orders = []
    for row in await cursor.fetchall():
        o = dict(row)
        items_cursor = await db.execute("SELECT * FROM order_items WHERE order_id=?", (o["id"],))
        o["items"] = [dict(i) for i in await items_cursor.fetchall()]
        orders.append(o)
    return orders

@app.get("/api/livreur/stats", tags=["Livreur"])
async def get_livreur_stats(user=Depends(get_livreur_user), db: aiosqlite.Connection = Depends(get_db)):
    total = (await (await db.execute(
        "SELECT COUNT(*) FROM order_attributions WHERE livreur_id=?", (user["id"],)
    )).fetchone())[0]
    livrees = (await (await db.execute(
        "SELECT COUNT(*) FROM orders o JOIN order_attributions oa ON o.id=oa.order_id WHERE oa.livreur_id=? AND o.status IN ('livree','paiement_recu')",
        (user["id"],)
    )).fetchone())[0]
    return {"total_missions": total, "livrees": livrees, "en_cours": total - livrees}

# ─── MARCHAND ─────────────────────────────────────────────────────────────────
@app.get("/api/merchant/products", tags=["Marchand"])
async def merchant_products(
    search: Optional[str] = None,
    user=Depends(get_merchant_or_admin),
    db: aiosqlite.Connection = Depends(get_db)
):
    where = "p.seller_id = ? AND p.is_active = 1"
    params = [user["id"]]
    if search:
        where += " AND (p.name LIKE ? OR p.id LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    cursor = await db.execute(f"""
        SELECT p.*, c.name as category_name FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        WHERE {where} ORDER BY p.created_at DESC
    """, params)
    products = await cursor.fetchall()
    result = []
    for p in products:
        p_dict = dict(p)
        img_cursor = await db.execute(
            "SELECT image_path FROM product_images WHERE product_id=? ORDER BY is_main DESC LIMIT 1",
            (p_dict["id"],)
        )
        img = await img_cursor.fetchone()
        p_dict["main_image"] = img["image_path"] if img else None
        result.append(p_dict)
    return result

@app.get("/api/merchant/orders", tags=["Marchand"])
async def merchant_orders(user=Depends(get_merchant_or_admin), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT o.*, oa.attributed_at,
               uc.full_name as client_name, uc.phone as client_phone,
               ul.full_name as livreur_name, ul.phone as livreur_phone
        FROM orders o
        LEFT JOIN order_attributions oa ON o.id = oa.order_id
        LEFT JOIN users uc ON o.user_id = uc.id
        LEFT JOIN users ul ON oa.livreur_id = ul.id
        WHERE o.seller_id=?
        ORDER BY o.created_at DESC
    """, (user["id"],))
    orders = []
    for row in await cursor.fetchall():
        o = dict(row)
        items_cursor = await db.execute("SELECT * FROM order_items WHERE order_id=?", (o["id"],))
        o["items"] = [dict(i) for i in await items_cursor.fetchall()]
        orders.append(o)
    return orders

@app.get("/api/merchant/stats", tags=["Marchand"])
async def merchant_stats(user=Depends(get_merchant_or_admin), db: aiosqlite.Connection = Depends(get_db)):
    prod = (await (await db.execute(
        "SELECT COUNT(*) FROM products WHERE seller_id=? AND is_active=1", (user["id"],)
    )).fetchone())[0]
    likes = (await (await db.execute(
        "SELECT COALESCE(SUM(likes),0) FROM products WHERE seller_id=? AND is_active=1", (user["id"],)
    )).fetchone())[0]
    views = (await (await db.execute(
        "SELECT COALESCE(SUM(views),0) FROM products WHERE seller_id=? AND is_active=1", (user["id"],)
    )).fetchone())[0]
    orders = (await (await db.execute(
        "SELECT COUNT(*), COALESCE(SUM(total_price),0) FROM orders WHERE seller_id=?", (user["id"],)
    )).fetchone())
    return {
        "products": prod, "total_likes": likes, "total_views": views,
        "total_orders": orders[0], "total_revenue": orders[1]
    }

# ─── NOTIFICATIONS ────────────────────────────────────────────────────────────
@app.get("/api/notifications", tags=["Notifications"])
async def get_notifications(user=Depends(get_current_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT * FROM notifications WHERE user_id=?
        ORDER BY created_at DESC LIMIT 50
    """, (user["id"],))
    notifs = [dict(n) for n in await cursor.fetchall()]
    unread = sum(1 for n in notifs if not n["is_read"])
    return {"notifications": notifs, "unread": unread}

@app.post("/api/notifications/read-all", tags=["Notifications"])
async def mark_all_read(user=Depends(get_current_user), db: aiosqlite.Connection = Depends(get_db)):
    await db.execute("UPDATE notifications SET is_read=1 WHERE user_id=?", (user["id"],))
    await db.commit()
    return {"message": "Toutes les notifications lues"}

# ─── ADMIN ────────────────────────────────────────────────────────────────────
@app.get("/api/admin/stats", tags=["Admin"])
async def get_admin_stats(user=Depends(get_admin_user), db: aiosqlite.Connection = Depends(get_db)):
    prod = (await (await db.execute("SELECT COUNT(*) FROM products WHERE is_active=1")).fetchone())[0]
    orders_row = await (await db.execute("SELECT COUNT(*), COALESCE(SUM(total_price),0) FROM orders")).fetchone()
    clients = (await (await db.execute("SELECT COUNT(*) FROM users WHERE role='client' AND is_active=1")).fetchone())[0]
    merchants = (await (await db.execute("SELECT COUNT(*) FROM users WHERE role='merchant' AND is_active=1")).fetchone())[0]
    livreurs = (await (await db.execute("SELECT COUNT(*) FROM users WHERE role='livreur' AND is_active=1")).fetchone())[0]
    pending = (await (await db.execute("SELECT COUNT(*) FROM orders WHERE status='validee'")).fetchone())[0]
    return {
        "products": prod, "orders": orders_row[0] or 0,
        "users": clients, "merchants": merchants, "livreurs": livreurs,
        "revenue": orders_row[1] or 0, "pending_orders": pending
    }

@app.get("/api/admin/clients", tags=["Admin"])
async def get_admin_clients(
    search: Optional[str] = None,
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    where = "WHERE role='client' AND is_active=1"
    params = []
    if search:
        where += " AND (phone LIKE ? OR full_name LIKE ?)"
        params = [f"%{search}%", f"%{search}%"]
    cursor = await db.execute(
        f"SELECT id, phone, full_name, birth_date, temp_code, suspended, created_at FROM users {where} ORDER BY created_at DESC",
        params
    )
    return [dict(u) for u in await cursor.fetchall()]

@app.get("/api/admin/merchants", tags=["Admin"])
async def get_admin_merchants(
    search: Optional[str] = None,
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    where = "WHERE role='merchant' AND is_active=1"
    params = []
    if search:
        where += " AND (phone LIKE ? OR full_name LIKE ?)"
        params = [f"%{search}%", f"%{search}%"]
    cursor = await db.execute(
        f"""SELECT id, phone, full_name, birth_date, temp_code, suspended,
            kyc_id_front, kyc_id_back, kyc_selfie, kyc_status, store_description, created_at
            FROM users {where} ORDER BY created_at DESC""",
        params
    )
    return [dict(u) for u in await cursor.fetchall()]

@app.get("/api/admin/livreurs", tags=["Admin"])
async def get_admin_livreurs(
    search: Optional[str] = None,
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    where = "WHERE role='livreur' AND is_active=1"
    params = []
    if search:
        where += " AND (phone LIKE ? OR full_name LIKE ?)"
        params = [f"%{search}%", f"%{search}%"]
    cursor = await db.execute(
        f"SELECT id, phone, full_name, suspended, last_lat, last_lng, last_gps_at, created_at FROM users {where} ORDER BY created_at DESC",
        params
    )
    return [dict(u) for u in await cursor.fetchall()]

@app.get("/api/admin/orders", tags=["Admin"])
async def get_admin_orders(
    search: Optional[str] = None,
    status: Optional[str] = None,
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    where_clauses = []
    params = []
    if search:
        where_clauses.append("(o.order_number LIKE ? OR u.phone LIKE ? OR u.full_name LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%", f"%{search}%"])
    if status:
        where_clauses.append("o.status = ?")
        params.append(status)
    where = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
    cursor = await db.execute(f"""
        SELECT o.*, u.phone as user_phone, u.full_name as client_name,
               ul.full_name as livreur_name, ul.phone as livreur_phone,
               um.full_name as merchant_name
        FROM orders o
        LEFT JOIN users u ON o.user_id = u.id
        LEFT JOIN order_attributions oa ON o.id = oa.order_id
        LEFT JOIN users ul ON oa.livreur_id = ul.id
        LEFT JOIN users um ON o.seller_id = um.id
        {where}
        ORDER BY o.created_at DESC
    """, params)
    orders = []
    for row in await cursor.fetchall():
        o = dict(row)
        items_cursor = await db.execute("SELECT * FROM order_items WHERE order_id=?", (o["id"],))
        o["items"] = [dict(i) for i in await items_cursor.fetchall()]
        orders.append(o)
    return orders

@app.get("/api/admin/merchants/{merchant_id}/products", tags=["Admin"])
async def get_merchant_products_admin(merchant_id: str, user=Depends(get_admin_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT p.*, c.name as category_name, u.phone as seller_phone, u.full_name as seller_name
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        LEFT JOIN users u ON p.seller_id = u.id
        WHERE p.seller_id = ? AND p.is_active = 1
        ORDER BY p.created_at DESC
    """, (merchant_id,))
    products = await cursor.fetchall()
    result = []
    for p in products:
        p_dict = dict(p)
        img_cursor = await db.execute(
            "SELECT image_path FROM product_images WHERE product_id=? ORDER BY is_main DESC LIMIT 1",
            (p_dict["id"],)
        )
        img = await img_cursor.fetchone()
        p_dict["main_image"] = img["image_path"] if img else None
        result.append(p_dict)
    return result

@app.get("/api/admin/qr-scans", tags=["Admin"])
async def get_qr_scans(user=Depends(get_admin_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT qs.*, u.full_name as scanner_name, u.phone as scanner_phone, u.role as scanner_role,
               o.order_number
        FROM qr_scans qs
        LEFT JOIN users u ON qs.scanner_id = u.id
        LEFT JOIN orders o ON qs.order_id = o.id
        ORDER BY qs.scanned_at DESC LIMIT 100
    """)
    return [dict(s) for s in await cursor.fetchall()]

@app.post("/api/admin/suspend", tags=["Admin"])
async def suspend_or_activate(
    data: SuspendAction,
    user=Depends(get_admin_user),
    db: aiosqlite.Connection = Depends(get_db)
):
    if data.code != SUSPEND_CODE:
        raise HTTPException(status_code=403, detail="Code incorrect")
    cursor = await db.execute("SELECT * FROM users WHERE id = ? AND role != 'admin'", (data.user_id,))
    target = await cursor.fetchone()
    if not target:
        raise HTTPException(status_code=404, detail="Utilisateur introuvable")
    if data.action == "suspend":
        await db.execute(
            "UPDATE users SET suspended=1, suspended_at=? WHERE id=?",
            (datetime.utcnow().isoformat(), data.user_id)
        )
        await db.commit()
        await manager.send_to_user(data.user_id, {"event": "account_suspended"})
        return {"message": "Compte suspendu"}
    elif data.action == "activate":
        await db.execute("UPDATE users SET suspended=0, suspended_at=NULL WHERE id=?", (data.user_id,))
        await db.commit()
        await manager.send_to_user(data.user_id, {"event": "account_activated"})
        return {"message": "Compte réactivé"}
    raise HTTPException(status_code=400, detail="Action invalide")

@app.get("/api/admin/products-by-likes", tags=["Admin"])
async def get_products_by_likes(user=Depends(get_admin_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT p.id, p.name, p.likes, p.views, p.price, c.name as category_name,
               u.full_name as seller_name, u.phone as seller_phone
        FROM products p
        LEFT JOIN categories c ON p.category_id = c.id
        LEFT JOIN users u ON p.seller_id = u.id
        WHERE p.is_active = 1 ORDER BY p.likes DESC, p.views DESC LIMIT 20
    """)
    return [dict(p) for p in await cursor.fetchall()]

# ─── CHAT ─────────────────────────────────────────────────────────────────────
@app.post("/api/chat/send", tags=["Chat"])
async def send_message(msg: ChatMessage, user=Depends(get_current_user), db: aiosqlite.Connection = Depends(get_db)):
    is_admin = user["role"] == "admin"
    receiver_id = msg.receiver_id if (is_admin or user["role"] in ("merchant", "livreur")) else None
    await db.execute("""
        INSERT INTO chat_messages (sender_id, receiver_id, message, is_from_admin, sender_role)
        VALUES (?, ?, ?, ?, ?)
    """, (user["id"], receiver_id, msg.message, 1 if is_admin else 0, user["role"]))
    await db.commit()

    payload = {
        "event": "new_message",
        "data": {
            "sender_id": user["id"], "sender_name": user["full_name"] or user["phone"],
            "sender_phone": user["phone"], "sender_role": user["role"],
            "message": msg.message, "is_from_admin": is_admin,
            "created_at": datetime.utcnow().isoformat()
        }
    }
    if is_admin and receiver_id:
        await manager.send_to_user(receiver_id, payload)
    else:
        await manager.send_to_admins(payload)
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
                SELECT cm.*, u.full_name as sender_name, u.phone as sender_phone, u.role as sender_role_info
                FROM chat_messages cm LEFT JOIN users u ON cm.sender_id = u.id
                WHERE (cm.sender_id = ? AND cm.is_from_admin = 0)
                   OR (cm.receiver_id = ? AND cm.is_from_admin = 1)
                ORDER BY cm.created_at ASC
            """, (with_user, with_user))
        else:
            cursor = await db.execute("""
                SELECT cm.*, u.full_name as sender_name, u.phone as sender_phone, u.role as sender_role_info
                FROM chat_messages cm LEFT JOIN users u ON cm.sender_id = u.id
                WHERE cm.is_from_admin = 0 ORDER BY cm.created_at DESC LIMIT 100
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
    if user["role"] != "admin":
        await db.execute("UPDATE chat_messages SET is_read=1 WHERE receiver_id=?", (user["id"],))
        await db.commit()
    return [dict(m) for m in msgs]

@app.get("/api/chat/conversations", tags=["Chat"])
async def get_conversations(user=Depends(get_admin_user), db: aiosqlite.Connection = Depends(get_db)):
    cursor = await db.execute("""
        SELECT DISTINCT u.id, u.phone, u.full_name, u.role as user_role,
               COUNT(CASE WHEN cm.is_read=0 AND cm.is_from_admin=0 THEN 1 END) as unread,
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
    prod = (await (await db.execute("SELECT COUNT(*) FROM products WHERE is_active=1")).fetchone())[0]
    clients = (await (await db.execute("SELECT COUNT(*) FROM users WHERE role='client'")).fetchone())[0]
    return {"products": prod, "clients": clients}

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

@app.get("/merchant")
async def merchant_panel():
    if os.path.exists("merchant.html"):
        return FileResponse("merchant.html")
    raise HTTPException(status_code=404, detail="merchant.html introuvable")

@app.get("/livreur")
async def livreur_panel():
    if os.path.exists("livreur.html"):
        return FileResponse("livreur.html")
    raise HTTPException(status_code=404, detail="livreur.html introuvable")

@app.get("/")
async def accueil():
    if os.path.exists("index.html"):
        return FileResponse("index.html")
    return {"message": "Digital Fashion Store API v3.0", "status": "online"}

@app.get("/api/health")
async def health_check():
    return {"status": "🟢 API en ligne", "version": "3.0.0"}

if __name__ == "__main__":
    import uvicorn
    port = int(os.getenv("PORT", 3000))
    uvicorn.run(app, host="0.0.0.0", port=port, log_level="info")
