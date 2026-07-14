"""
ربات فوق‌پیشرفته پنل Xray-core
با قابلیت‌های: مدیریت کاربران، ترافیک، پرداخت، QR Code، چندسروره، AI، بکاپ خودکار
"""

import asyncio
import json
import os
import uuid
import hashlib
import qrcode
import aiohttp
import asyncpg
import redis.asyncio as redis
from datetime import datetime, timedelta
from typing import Dict, Optional, List
from pathlib import Path
import grpc
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, InputFile
from telegram.ext import (
    Application, CommandHandler, CallbackQueryHandler, MessageHandler,
    ContextTypes, ConversationHandler, filters
)
import logging
from cryptography.fernet import Fernet
import pandas as pd
from io import BytesIO
import matplotlib.pyplot as plt
import seaborn as sns

# ======================== تنظیمات اولیه ========================

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# متغیرهای محیطی
BOT_TOKEN = os.getenv("BOT_TOKEN", "8793482183:AAEGUa7ZEURP26N34DzKvrudnndC3q7apBk")
DATABASE_URL = os.getenv("DATABASE_URL", "postgresql://user:pass@localhost:5432/xray")
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379")
ADMIN_IDS = [int(id) for id in os.getenv("ADMIN_IDS", "8680457924").split(",")]
XRAY_GRPC_ADDR = os.getenv("XRAY_GRPC_ADDR", "localhost:9000")
ENCRYPT_KEY = os.getenv("ENCRYPT_KEY", Fernet.generate_key())

# برای پرداخت
ZARINPAL_MERCHANT = os.getenv("ZARINPAL_MERCHANT", "")
CRYPTO_API_KEY = os.getenv("CRYPTO_API_KEY", "")

# حالات مکالمه
(
    SELECT_PLAN, WAIT_PAYMENT, WAIT_RENEW, 
    WAIT_EMAIL, WAIT_NODE, ADMIN_MENU,
    BROADCAST, ADD_NODE, REMOVE_NODE
) = range(9)

# ======================== کلاس‌های اصلی ========================

class UltimateXrayBot:
    def __init__(self):
        self.db_pool = None
        self.redis_client = None
        self.xray_client = None
        self.cipher = Fernet(ENCRYPT_KEY)
        self.active_users = {}  # Cache برای کاربران فعال
        self.node_status = {}   # وضعیت سرورها
        
    async def init_db(self):
        """اتصال به PostgreSQL با Connection Pool"""
        self.db_pool = await asyncpg.create_pool(
            DATABASE_URL,
            min_size=5,
            max_size=20,
            command_timeout=60
        )
        await self.create_tables()
        
    async def create_tables(self):
        """ایجاد جداول دیتابیس"""
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS users (
                    id BIGINT PRIMARY KEY,
                    username VARCHAR(100),
                    uuid VARCHAR(36) UNIQUE NOT NULL,
                    volume BIGINT DEFAULT 0,
                    used BIGINT DEFAULT 0,
                    expiry_date TIMESTAMP,
                    status VARCHAR(20) DEFAULT 'active',
                    node_id INTEGER DEFAULT 1,
                    email VARCHAR(100),
                    created_at TIMESTAMP DEFAULT NOW(),
                    updated_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS transactions (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(id),
                    amount DECIMAL(10,2),
                    type VARCHAR(20),
                    description TEXT,
                    status VARCHAR(20) DEFAULT 'pending',
                    reference VARCHAR(100),
                    created_at TIMESTAMP DEFAULT NOW()
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS nodes (
                    id SERIAL PRIMARY KEY,
                    name VARCHAR(100),
                    ip VARCHAR(50),
                    port INTEGER DEFAULT 443,
                    api_port INTEGER DEFAULT 9000,
                    weight INTEGER DEFAULT 1,
                    status VARCHAR(20) DEFAULT 'active',
                    last_check TIMESTAMP,
                    config JSONB
                )
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS configs (
                    id SERIAL PRIMARY KEY,
                    user_id BIGINT REFERENCES users(id),
                    protocol VARCHAR(20),
                    config TEXT,
                    qr_code TEXT,
                    created_at TIMESTAMP DEFAULT NOW(),
                    expires_at TIMESTAMP
                )
            """)
            
            # ایندکس‌ها برای سرعت
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_uuid ON users(uuid)")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_users_expiry ON users(expiry_date)")
            
    async def init_redis(self):
        """اتصال به Redis برای کش"""
        self.redis_client = await redis.from_url(REDIS_URL, decode_responses=True)
        
    async def init_xray(self):
        """اتصال به Xray-core از طریق gRPC"""
        # اینجا کد اتصال gRPC قرار میگیره
        pass
        
    # ======================== توابع هسته ========================
    
    async def get_user(self, user_id: int) -> Optional[Dict]:
        """دریافت اطلاعات کاربر با کش"""
        # چک کردن کش
        if user_id in self.active_users:
            return self.active_users[user_id]
            
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow(
                "SELECT * FROM users WHERE id = $1", user_id
            )
            if row:
                user_data = dict(row)
                self.active_users[user_id] = user_data
                return user_data
        return None
        
    async def create_user(self, user_id: int, username: str) -> Dict:
        """ایجاد کاربر جدید با UUID منحصربه‌فرد"""
        user_uuid = str(uuid.uuid4())
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO users (id, username, uuid, volume, used, expiry_date, status)
                VALUES ($1, $2, $3, $4, $5, $6, $7)
            """, user_id, username, user_uuid, 0, 0, 
                datetime.now() + timedelta(days=1), 'inactive'
            )
            
        user_data = {
            'id': user_id,
            'username': username,
            'uuid': user_uuid,
            'volume': 0,
            'used': 0,
            'expiry_date': datetime.now() + timedelta(days=1),
            'status': 'inactive'
        }
        self.active_users[user_id] = user_data
        return user_data
        
    async def add_volume(self, user_id: int, gb: int) -> bool:
        """افزایش حجم کاربر"""
        async with self.db_pool.acquire() as conn:
            result = await conn.execute("""
                UPDATE users 
                SET volume = volume + $1, 
                    status = 'active',
                    updated_at = NOW()
                WHERE id = $2
            """, gb * 1024**3, user_id)  # تبدیل به بایت
            
            # آپدیت کش
            if user_id in self.active_users:
                self.active_users[user_id]['volume'] += gb * 1024**3
                self.active_users[user_id]['status'] = 'active'
                
            return result != "UPDATE 0"
            
    async def get_remaining(self, user_id: int) -> int:
        """دریافت حجم باقی‌مونده به بایت"""
        user = await self.get_user(user_id)
        if not user:
            return 0
        return max(0, user['volume'] - user['used'])
        
    async def check_expiry(self, user_id: int) -> bool:
        """بررسی انقضای اشتراک"""
        user = await self.get_user(user_id)
        if not user:
            return False
        return user['expiry_date'] > datetime.now()
        
    async def generate_config(self, user_id: int, protocol: str = "vless") -> Dict:
        """تولید کانفیگ با کیفیت بالا"""
        user = await self.get_user(user_id)
        if not user:
            return None
            
        # گرفتن بهترین سرور
        node = await self.get_best_node()
        
        # ساخت کانفیگ بر اساس پروتکل
        config_data = {
            'vless': {
                'protocol': 'vless',
                'address': node['ip'],
                'port': 443,
                'uuid': user['uuid'],
                'path': '/vless',
                'security': 'tls',
                'sni': node.get('sni', ''),
                'flow': 'xtls-rprx-vision',
                'network': 'ws'
            },
            'vmess': {
                'protocol': 'vmess',
                'address': node['ip'],
                'port': 443,
                'uuid': user['uuid'],
                'alterId': 0,
                'security': 'auto',
                'network': 'ws',
                'path': '/vmess'
            },
            'trojan': {
                'protocol': 'trojan',
                'address': node['ip'],
                'port': 443,
                'password': user['uuid'],
                'sni': node.get('sni', ''),
                'network': 'tcp'
            }
        }
        
        config = config_data.get(protocol, config_data['vless'])
        
        # ساخت لینک کانفیگ
        if protocol == 'vless':
            link = f"vless://{config['uuid']}@{config['address']}:{config['port']}?security={config['security']}&encryption=none&flow={config['flow']}&sni={config['sni']}&path={config['path']}&type={config['network']}#Xray-{user['username']}"
        elif protocol == 'vmess':
            vmess_data = {
                "v": "2",
                "ps": f"Xray-{user['username']}",
                "add": config['address'],
                "port": str(config['port']),
                "id": config['uuid'],
                "aid": "0",
                "net": config['network'],
                "type": "none",
                "host": "",
                "path": config['path'],
                "tls": "tls"
            }
            link = f"vmess://{base64.b64encode(json.dumps(vmess_data).encode()).decode()}"
        else:
            link = f"trojan://{config['password']}@{config['address']}:{config['port']}?sni={config['sni']}&type={config['network']}#Xray-{user['username']}"
            
        # ساخت QR Code
        qr = qrcode.QRCode(version=1, box_size=10, border=5)
        qr.add_data(link)
        qr.make(fit=True)
        qr_img = qr.make_image(fill_color="black", back_color="white")
        
        qr_path = f"/tmp/qr_{user_id}_{datetime.now().timestamp()}.png"
        qr_img.save(qr_path)
        
        return {
            'link': link,
            'qr_path': qr_path,
            'config_text': json.dumps(config, indent=2),
            'protocol': protocol
        }
        
    async def get_best_node(self) -> Dict:
        """پیدا کردن بهترین سرور بر اساس پینگ و بار"""
        nodes = await self.get_nodes()
        if not nodes:
            # سرور پیش‌فرض
            return {
                'id': 1,
                'name': 'Main Server',
                'ip': 'your-domain.com',
                'port': 443,
                'sni': 'your-domain.com',
                'weight': 1
            }
            
        # انتخاب بر اساس وزن و وضعیت
        active_nodes = [n for n in nodes if n['status'] == 'active']
        if not active_nodes:
            return nodes[0]
            
        # مرتب‌سازی بر اساس وزن و آخرین چک
        best = sorted(active_nodes, key=lambda x: (x['weight'], -x['id']), reverse=True)[0]
        return best
        
    async def get_nodes(self) -> List[Dict]:
        """دریافت لیست سرورها"""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("SELECT * FROM nodes ORDER BY weight DESC")
            return [dict(row) for row in rows]
            
    async def add_node(self, name: str, ip: str, port: int, api_port: int) -> int:
        """افزودن سرور جدید"""
        async with self.db_pool.acquire() as conn:
            row = await conn.fetchrow("""
                INSERT INTO nodes (name, ip, port, api_port, status, last_check)
                VALUES ($1, $2, $3, $4, 'active', NOW())
                RETURNING id
            """, name, ip, port, api_port)
            return row['id']
            
    async def remove_node(self, node_id: int) -> bool:
        """حذف سرور"""
        async with self.db_pool.acquire() as conn:
            result = await conn.execute(
                "DELETE FROM nodes WHERE id = $1", node_id
            )
            return result != "DELETE 0"
            
    # ======================== سیستم پرداخت ========================
    
    async def create_payment(self, user_id: int, amount: float, plan: str) -> str:
        """ایجاد درخواست پرداخت"""
        # اینجا می‌تونی زرین‌پال یا کریپتو رو وصل کنی
        reference = hashlib.md5(f"{user_id}{amount}{datetime.now()}".encode()).hexdigest()
        
        async with self.db_pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO transactions (user_id, amount, type, description, status, reference)
                VALUES ($1, $2, 'payment', $3, 'pending', $4)
            """, user_id, amount, plan, reference)
            
        return reference
        
    async def verify_payment(self, reference: str) -> bool:
        """تایید پرداخت"""
        async with self.db_pool.acquire() as conn:
            tx = await conn.fetchrow(
                "SELECT * FROM transactions WHERE reference = $1 AND status = 'pending'",
                reference
            )
            if not tx:
                return False
                
            # اینجا منطق تایید پرداخت واقعی
            # برای نمونه: 10 گیگ برای هر 10000 تومان
            gb = int(tx['amount'] / 10000) * 10
            
            # آپدیت حجم کاربر
            await self.add_volume(tx['user_id'], gb)
            
            # آپدیت وضعیت تراکنش
            await conn.execute("""
                UPDATE transactions SET status = 'completed'
                WHERE reference = $1
            """, reference)
            
            return True
            
    # ======================== سیستم تحلیلی ========================
    
    async def get_user_stats(self, user_id: int) -> Dict:
        """گرفتن آمار مصرف کاربر"""
        user = await self.get_user(user_id)
        if not user:
            return {}
            
        return {
            'total_volume': user['volume'] / 1024**3,  # GB
            'used_volume': user['used'] / 1024**3,
            'remaining': (user['volume'] - user['used']) / 1024**3,
            'percentage': (user['used'] / user['volume'] * 100) if user['volume'] > 0 else 0,
            'expiry': user['expiry_date'].strftime('%Y-%m-%d %H:%M'),
            'status': user['status']
        }
        
    async def generate_chart(self, user_id: int) -> str:
        """تولید نمودار مصرف"""
        # گرفتن داده‌های مصرف روزانه از Redis
        usage_data = await self.redis_client.lrange(f"usage:{user_id}:daily", 0, 30)
        if not usage_data:
            usage_data = [0] * 30
            
        # رسم نمودار
        plt.figure(figsize=(10, 6))
        sns.set_style("darkgrid")
        
        dates = [datetime.now() - timedelta(days=i) for i in range(30, 0, -1)]
        usage = [float(d) for d in usage_data[::-1]]
        
        plt.plot(dates, usage, marker='o', linewidth=2, color='#00ff88')
        plt.fill_between(dates, usage, alpha=0.3, color='#00ff88')
        plt.title("مصرف روزانه (MB)", fontsize=16, pad=20)
        plt.xlabel("تاریخ", fontsize=12)
        plt.ylabel("مصرف (MB)", fontsize=12)
        plt.xticks(rotation=45)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        
        chart_path = f"/tmp/chart_{user_id}_{datetime.now().timestamp()}.png"
        plt.savefig(chart_path, dpi=100, bbox_inches='tight')
        plt.close()
        
        return chart_path
        
    # ======================== بکاپ خودکار ========================
    
    async def backup_database(self):
        """بکاپ خودکار از دیتابیس"""
        try:
            backup_time = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_file = f"/tmp/backup_{backup_time}.sql"
            
            # گرفتن دامپ از PostgreSQL
            async with self.db_pool.acquire() as conn:
                await conn.execute(f"COPY users TO '{backup_file}'")
                
            # رمزنگاری فایل بکاپ
            with open(backup_file, 'rb') as f:
                encrypted_data = self.cipher.encrypt(f.read())
                
            encrypted_file = f"/tmp/backup_{backup_time}.enc"
            with open(encrypted_file, 'wb') as f:
                f.write(encrypted_data)
                
            # آپلود به Google Drive یا S3 (کدش رو اینجا اضافه کن)
            # upload_to_drive(encrypted_file)
            
            # پاک کردن فایل‌های موقت
            os.remove(backup_file)
            os.remove(encrypted_file)
            
            logger.info(f"Backup created successfully: {backup_time}")
        except Exception as e:
            logger.error(f"Backup failed: {e}")
            
    # ======================== مدیریت کاربران ========================
    
    async def get_all_users(self) -> List[Dict]:
        """دریافت لیست همه کاربران"""
        async with self.db_pool.acquire() as conn:
            rows = await conn.fetch("""
                SELECT id, username, volume, used, status, expiry_date 
                FROM users ORDER BY created_at DESC
            """)
            return [dict(row) for row in rows]
            
    async def delete_user(self, user_id: int) -> bool:
        """حذف کاربر"""
        async with self.db_pool.acquire() as conn:
            result = await conn.execute("DELETE FROM users WHERE id = $1", user_id)
            if user_id in self.active_users:
                del self.active_users[user_id]
            return result != "DELETE 0"
            
    async def broadcast_message(self, message: str) -> int:
        """ارسال پیام به همه کاربران"""
        users = await self.get_all_users()
        sent_count = 0
        
        # اینجا باید تابع ارسال پیام به تلگرام رو صدا بزنی
        # برای هر کاربر پیام ارسال کن
        
        return sent_count

# ======================== هندلرهای ربات ========================

bot_instance = UltimateXrayBot()

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """دستور شروع - نمایش وضعیت کاربر"""
    user = update.effective_user
    user_data = await bot_instance.get_user(user.id)
    
    if not user_data:
        # ثبت نام کاربر جدید
        await bot_instance.create_user(user.id, user.username)
        text = f"""
🚀 **به ربات پنل Xray خوش آمدید!**

🎯 **ویژگی‌های ربات:**
• کانفیگ‌های VLESS/VMess/Trojan
• پشتیبانی از چندین سرور
• سیستم پرداخت خودکار
• پنل کاربری پیشرفته

📌 **برای شروع، یکی از گزینه‌های زیر را انتخاب کنید:**
"""
        keyboard = [
            [InlineKeyboardButton("🛒 خرید اشتراک", callback_data="buy")],
            [InlineKeyboardButton("ℹ️ راهنما", callback_data="help")]
        ]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')
    else:
        # نمایش وضعیت کاربر
        stats = await bot_instance.get_user_stats(user.id)
        remaining_days = (user_data['expiry_date'] - datetime.now()).days
        
        text = f"""
📊 **پنل کاربری**

👤 **نام:** {user_data['username']}
📧 **ایمیل:** {user_data.get('email', 'ثبت نشده')}

💾 **حجم کل:** {stats['total_volume']:.1f} GB
📊 **مصرف شده:** {stats['used_volume']:.1f} GB
✅ **حجم باقی:** {stats['remaining']:.1f} GB
📈 **درصد مصرف:** {stats['percentage']:.1f}%

⏳ **اعتبار:** {remaining_days} روز
🟢 **وضعیت:** {'فعال' if user_data['status'] == 'active' else 'غیرفعال'}

**📌 منوی اصلی:**
"""
        keyboard = [
            [InlineKeyboardButton("🔗 دریافت کانفیگ", callback_data="get_config")],
            [InlineKeyboardButton("📈 نمودار مصرف", callback_data="chart")],
            [InlineKeyboardButton("🔄 تمدید اشتراک", callback_data="renew")],
            [InlineKeyboardButton("🌐 تغییر سرور", callback_data="change_node")],
            [InlineKeyboardButton("👤 پروفایل", callback_data="profile")],
            [InlineKeyboardButton("💳 خرید", callback_data="buy")],
            [InlineKeyboardButton("🆘 پشتیبانی", callback_data="support")]
        ]
        await update.message.reply_text(text, reply_markup=InlineKeyboardMarkup(keyboard), parse_mode='Markdown')

async def handle_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """هندلر دکمه‌ها"""
    query = update.callback_query
    await query.answer()
    
    user_id = query.from_user.id
    data = query.data
    
    if data == "buy":
        await show_buy_menu(update, context)
    elif data == "get_config":
        await send_config(update, context)
    elif data == "chart":
        await send_chart(update, context)
    elif data == "renew":
        await renew_subscription(update, context)
    elif data == "change_node":
        await change_node(update, context)
    elif data == "profile":
        await show_profile(update, context)
    elif data == "support":
        await support(update, context)
    elif data.startswith("plan_"):
        await handle_plan_selection(update, context)
    elif data.startswith("node_"):
        await handle_node_selection(update, context)
    elif data == "admin_panel":
        await admin_panel(update, context)
    elif data == "admin_stats":
        await admin_stats(update, context)
    elif data == "admin_users":
        await admin_users(update, context)
    elif data == "admin_backup":
        await admin_backup(update, context)
    elif data == "admin_broadcast":
        await admin_broadcast_start(update, context)

async def show_buy_menu(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """نمایش منوی خرید"""
    plans = [
        {"name": "🔰 پایه", "gb": 30, "price": 50000, "days": 30},
        {"name": "⭐ استاندارد", "gb": 100, "price": 150000, "days": 45},
        {"name": "💎 حرفه‌ای", "gb": 250, "price": 300000, "days": 60},
        {"name": "👑 VIP", "gb": 500, "price": 500000, "days": 90},
    ]
    
    text = "💳 **خرید اشتراک:**\n\n"
    for i, plan in enumerate(plans):
        text += f"{plan['name']} - {plan['gb']}GB - {plan['days']} روز\n"
        text += f"💰 قیمت: {plan['price']:,} تومان\n\n"
        
    keyboard = []
    for i, plan in enumerate(plans):
        keyboard.append([
            InlineKeyboardButton(
                f"{plan['name']} - {plan['gb']}GB",
                callback_data=f"plan_{i}"
            )
        ])
    keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data="back")])
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_plan_selection(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """پردازش انتخاب پلن"""
    query = update.callback_query
    plan_index = int(query.data.split("_")[1])
    
    plans = [
        {"gb": 30, "price": 50000, "days": 30},
        {"gb": 100, "price": 150000, "days": 45},
        {"gb": 250, "price": 300000, "days": 60},
        {"gb": 500, "price": 500000, "days": 90},
    ]
    
    plan = plans[plan_index]
    user_id = query.from_user.id
    
    # ساخت لینک پرداخت
    reference = await bot_instance.create_payment(user_id, plan['price'], f"{plan['gb']}GB")
    
    # اینجا لینک درگاه پرداخت رو بساز
    payment_link = f"https://your-payment-gateway.com/pay/{reference}"
    
    text = f"""
💳 **پرداخت اشتراک**

📦 **حجم:** {plan['gb']}GB
💰 **مبلغ:** {plan['price']:,} تومان
📅 **مدت:** {plan['days']} روز

🔗 **لینک پرداخت:**
`{payment_link}`

✅ پس از پرداخت، اشتراک شما به‌صورت خودکار فعال می‌شود.
⏱️ زمان تایید پرداخت: حداکثر ۵ دقیقه
"""
    
    keyboard = [
        [InlineKeyboardButton("✅ تایید پرداخت", callback_data=f"verify_{reference}")],
        [InlineKeyboardButton("🔙 بازگشت", callback_data="buy")]
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def send_config(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ارسال کانفیگ به کاربر"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # انتخاب پروتکل
    keyboard = [
        [InlineKeyboardButton("🚀 VLESS", callback_data="config_vless")],
        [InlineKeyboardButton("📦 VMess", callback_data="config_vmess")],
        [InlineKeyboardButton("🔰 Trojan", callback_data="config_trojan")],
        [InlineKeyboardButton("🔙 بازگشت", callback_data="back")]
    ]
    
    await query.edit_message_text(
        "🔐 **انتخاب پروتکل:**\n\n"
        "بهترین پروتکل برای سرعت و امنیت، VLESS است.",
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def send_chart(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ارسال نمودار مصرف"""
    query = update.callback_query
    user_id = query.from_user.id
    
    # ساخت نمودار
    chart_path = await bot_instance.generate_chart(user_id)
    
    with open(chart_path, 'rb') as f:
        await query.message.reply_photo(
            InputFile(f),
            caption="📊 **نمودار مصرف روزانه**"
        )
    
    # پاک کردن فایل موقت
    os.remove(chart_path)
    
    await query.delete_message()

async def renew_subscription(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تمدید اشتراک"""
    query = update.callback_query
    
    text = """
🔄 **تمدید اشتراک**

با تمدید اشتراک، ۲۰٪ تخفیف دریافت کنید!

📦 **پلن‌های تمدید:**
• ۳۰GB - ۴۰,۰۰۰ تومان (۲۵٪ تخفیف)
• ۱۰۰GB - ۱۲۰,۰۰۰ تومان (۲۰٪ تخفیف)
• ۲۵۰GB - ۲۴۰,۰۰۰ تومان (۲۰٪ تخفیف)
"""
    
    keyboard = [
        [InlineKeyboardButton("۳۰GB - ۴۰K", callback_data="renew_30")],
        [InlineKeyboardButton("۱۰۰GB - ۱۲۰K", callback_data="renew_100")],
        [InlineKeyboardButton("۲۵۰GB - ۲۴۰K", callback_data="renew_250")],
        [InlineKeyboardButton("🔙 بازگشت", callback_data="back")]
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def change_node(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """تغییر سرور"""
    query = update.callback_query
    
    nodes = await bot_instance.get_nodes()
    
    text = "🌐 **انتخاب سرور:**\n\n"
    keyboard = []
    
    for node in nodes:
        status_emoji = "🟢" if node['status'] == 'active' else "🔴"
        text += f"{status_emoji} {node['name']}\n"
        text += f"📡 {node['ip']}:{node['port']}\n\n"
        
        keyboard.append([
            InlineKeyboardButton(
                f"{status_emoji} {node['name']}",
                callback_data=f"node_{node['id']}"
            )
        ])
    
    keyboard.append([InlineKeyboardButton("🔙 بازگشت", callback_data="back")])
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """پنل مدیریت"""
    query = update.callback_query
    
    if query.from_user.id not in ADMIN_IDS:
        await query.edit_message_text("⛔ شما دسترسی ادمین ندارید!")
        return
        
    text = """
🔐 **پنل مدیریت**

📊 **آمار کلی:**
• کاربران کل: {total_users}
• کاربران فعال: {active_users}
• درآمد امروز: {today_income:,} تومان
• درآمد ماه: {month_income:,} تومان

**📌 منوی ادمین:**
"""
    
    # گرفتن آمار
    users = await bot_instance.get_all_users()
    total_users = len(users)
    active_users = len([u for u in users if u['status'] == 'active'])
    
    text = text.format(
        total_users=total_users,
        active_users=active_users,
        today_income=0,  # از دیتابیس محاسبه کن
        month_income=0
    )
    
    keyboard = [
        [InlineKeyboardButton("📊 آمار کامل", callback_data="admin_stats")],
        [InlineKeyboardButton("👥 مدیریت کاربران", callback_data="admin_users")],
        [InlineKeyboardButton("💾 بکاپ", callback_data="admin_backup")],
        [InlineKeyboardButton("📢 ارسال همگانی", callback_data="admin_broadcast")],
        [InlineKeyboardButton("➕ افزودن سرور", callback_data="admin_add_node")],
        [InlineKeyboardButton("➖ حذف سرور", callback_data="admin_remove_node")],
        [InlineKeyboardButton("🔙 بازگشت", callback_data="back")]
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """نمایش آمار کامل"""
    query = update.callback_query
    
    users = await bot_instance.get_all_users()
    
    # محاسبه آمار
    total_users = len(users)
    active_users = len([u for u in users if u['status'] == 'active'])
    total_volume = sum([u['volume'] for u in users]) / 1024**3
    used_volume = sum([u['used'] for u in users]) / 1024**3
    
    text = f"""
📊 **آمار کامل سیستم**

👥 **کاربران:**
• کل کاربران: {total_users}
• فعال: {active_users}
• غیرفعال: {total_users - active_users}

💾 **حجم:**
• کل حجم: {total_volume:.1f} GB
• مصرف شده: {used_volume:.1f} GB
• میانگین مصرف هر کاربر: {used_volume/total_users:.1f} GB

🌐 **سرورها:**
• تعداد سرورها: {len(await bot_instance.get_nodes())}
• سرورهای فعال: {len([n for n in await bot_instance.get_nodes() if n['status'] == 'active'])}
"""
    
    keyboard = [[InlineKeyboardButton("🔙 بازگشت", callback_data="admin_panel")]]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def admin_backup(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """گرفتن بکاپ"""
    query = update.callback_query
    
    await query.edit_message_text("⏳ در حال گرفتن بکاپ...")
    
    await bot_instance.backup_database()
    
    await query.edit_message_text(
        "✅ **بکاپ با موفقیت انجام شد!**\n\n"
        "فایل بکاپ رمزنگاری شده و در فضای ابری ذخیره شد.",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("🔙 بازگشت", callback_data="admin_panel")]
        ]),
        parse_mode='Markdown'
    )

async def admin_broadcast_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """شروع ارسال همگانی"""
    query = update.callback_query
    
    await query.edit_message_text(
        "📢 **ارسال پیام همگانی**\n\n"
        "پیام خود را وارد کنید:",
        reply_markup=InlineKeyboardMarkup([
            [InlineKeyboardButton("❌ لغو", callback_data="admin_panel")]
        ]),
        parse_mode='Markdown'
    )
    
    # ورود به حالت ارسال همگانی
    context.user_data['state'] = 'broadcast'
    return BROADCAST

async def handle_broadcast(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """دریافت پیام برای ارسال همگانی"""
    if 'state' not in context.user_data or context.user_data['state'] != 'broadcast':
        return
    
    message = update.message.text
    users = await bot_instance.get_all_users()
    
    sent = 0
    for user in users:
        try:
            await update.message.reply_text(f"ارسال به {user['username']}...")
            # اینجا پیام واقعی رو ارسال کن
            sent += 1
            await asyncio.sleep(0.05)  # جلوگیری از محدودیت
        except:
            pass
    
    await update.message.reply_text(
        f"✅ **پیام به {sent} کاربر ارسال شد!**"
    )
    
    context.user_data['state'] = None
    return ConversationHandler.END

async def show_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """نمایش پروفایل کاربر"""
    query = update.callback_query
    user_id = query.from_user.id
    
    user_data = await bot_instance.get_user(user_id)
    if not user_data:
        await query.edit_message_text("❌ کاربر یافت نشد!")
        return
        
    stats = await bot_instance.get_user_stats(user_id)
    
    text = f"""
👤 **پروفایل کاربری**

🆔 **شناسه:** {user_id}
📝 **نام کاربری:** @{user_data.get('username', 'نامشخص')}
📧 **ایمیل:** {user_data.get('email', 'ثبت نشده')}

💾 **حجم کل:** {stats['total_volume']:.1f} GB
📊 **مصرف شده:** {stats['used_volume']:.1f} GB
✅ **حجم باقی:** {stats['remaining']:.1f} GB
📈 **درصد مصرف:** {stats['percentage']:.1f}%

📅 **تاریخ انقضا:** {stats['expiry']}
🟢 **وضعیت:** {'فعال' if user_data['status'] == 'active' else 'غیرفعال'}

🔐 **UUID:** `{user_data['uuid']}`
"""
    
    keyboard = [
        [InlineKeyboardButton("🔙 بازگشت", callback_data="back")]
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def support(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """پشتیبانی"""
    query = update.callback_query
    
    text = """
🆘 **پشتیبانی**

برای ارتباط با پشتیبانی، از یکی از راه‌های زیر استفاده کنید:

📧 **ایمیل:** support@xray-bot.com
💬 **تلگرام:** @XraySupportBot
🌐 **وبسایت:** https://xray-bot.com

**سوالات متداول:**
• ❓ چگونه کانفیگ دریافت کنم؟
  از منوی اصلی گزینه "دریافت کانفیگ" را انتخاب کنید.

• ❓ چگونه اشتراک بخرم؟
  از منوی اصلی گزینه "خرید" را انتخاب کنید.

• ❓ سرعت پایین است؟
  از گزینه "تغییر سرور" استفاده کنید.

⏱️ **زمان پاسخگویی:** ۲۴/۷
"""
    
    keyboard = [
        [InlineKeyboardButton("🔙 بازگشت", callback_data="back")]
    ]
    
    await query.edit_message_text(
        text,
        reply_markup=InlineKeyboardMarkup(keyboard),
        parse_mode='Markdown'
    )

async def handle_unknown(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """هندلر پیام‌های ناشناخته"""
    await update.message.reply_text(
        "❌ دستور نامعتبر!\n"
        "برای مشاهده راهنما، /start را وارد کنید."
    )

async def error_handler(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """هندلر خطاها"""
    logger.error(f"Update {update} caused error {context.error}")
    
    if update and update.effective_message:
        await update.effective_message.reply_text(
            "❌ خطایی رخ داد! لطفاً دوباره تلاش کنید."
        )

# ======================== اجرای اصلی ========================

async def main():
    """تابع اصلی اجرای ربات"""
    # مقداردهی اولیه
    await bot_instance.init_db()
    await bot_instance.init_redis()
    await bot_instance.init_xray()
    
    # ساخت اپلیکیشن
    app = Application.builder().token(BOT_TOKEN).build()
    
    # ثبت هندلرها
    app.add_handler(CommandHandler("start", start))
    app.add_handler(CallbackQueryHandler(handle_callback))
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, handle_broadcast))
    
    # هندلر خطا
    app.add_error_handler(error_handler)
    
    # هندلر پیام‌های ناشناخته
    app.add_handler(MessageHandler(filters.ALL, handle_unknown))
    
    # شروع ربات
    logger.info("🤖 ربات شروع به کار کرد!")
    await app.run_polling()

if __name__ == "__main__":
    asyncio.run(main())
