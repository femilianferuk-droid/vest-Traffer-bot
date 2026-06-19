import asyncio
import json
import os
import sys
from datetime import datetime

import asyncpg
from aiogram import Bot, Dispatcher, types
from aiogram.filters import Command
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError, ChatWriteForbiddenError
from telethon.sessions import StringSession

BOT_TOKEN = os.environ.get('BOT_TOKEN')
DATABASE_URL = os.environ.get('DATABASE_URL')

if not BOT_TOKEN or not DATABASE_URL:
    print("❌ Error: BOT_TOKEN and DATABASE_URL required")
    sys.exit(1)

API_ID = 32480523
API_HASH = '147839735c9fa4e83451209e9b55cfc5'

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()
db_pool = None

class VestTrafferWorker:
    def __init__(self):
        self.active_autoresponders = {}
        self.running_mailings = {}
    
    async def init_db(self):
        global db_pool
        db_pool = await asyncpg.create_pool(DATABASE_URL, min_size=5, max_size=20)
        
        async with db_pool.acquire() as conn:
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS users (
                    id SERIAL PRIMARY KEY,
                    username VARCHAR(255) UNIQUE NOT NULL,
                    password_hash VARCHAR(255) NOT NULL,
                    subscription_ends TIMESTAMP DEFAULT NULL
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS account (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    phone VARCHAR(20) NOT NULL,
                    session_string TEXT
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS mailing_task (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    account_id INTEGER REFERENCES account(id),
                    chats JSONB,
                    message TEXT,
                    delay INTEGER DEFAULT 10,
                    sent_count INTEGER DEFAULT 0,
                    total_messages INTEGER DEFAULT 0,
                    mailing_type VARCHAR(20) DEFAULT 'simultaneous',
                    status VARCHAR(20) DEFAULT 'Ожидает',
                    current_chat VARCHAR(255),
                    errors JSONB DEFAULT '[]'
                )
            ''')
            
            await conn.execute('''
                CREATE TABLE IF NOT EXISTS autoresponder (
                    id SERIAL PRIMARY KEY,
                    user_id INTEGER REFERENCES users(id),
                    account_id INTEGER REFERENCES account(id),
                    trigger_type VARCHAR(20) DEFAULT 'all',
                    keywords TEXT,
                    reply_text TEXT,
                    is_active BOOLEAN DEFAULT true,
                    response_count INTEGER DEFAULT 0,
                    last_response TIMESTAMP
                )
            ''')
            
            await conn.execute('ALTER TABLE mailing_task ADD COLUMN IF NOT EXISTS sent_count INTEGER DEFAULT 0')
            await conn.execute('ALTER TABLE mailing_task ADD COLUMN IF NOT EXISTS current_chat VARCHAR(255)')
            await conn.execute('ALTER TABLE mailing_task ADD COLUMN IF NOT EXISTS errors JSONB DEFAULT \'[]\'')
            await conn.execute('ALTER TABLE users ADD COLUMN IF NOT EXISTS subscription_ends TIMESTAMP DEFAULT NULL')
            await conn.execute('ALTER TABLE autoresponder ADD COLUMN IF NOT EXISTS response_count INTEGER DEFAULT 0')
            await conn.execute('ALTER TABLE autoresponder ADD COLUMN IF NOT EXISTS last_response TIMESTAMP')
    
    async def check_subscription(self, user_id):
        async with db_pool.acquire() as conn:
            user = await conn.fetchrow('SELECT subscription_ends FROM users WHERE id = $1', user_id)
            if not user or not user['subscription_ends']:
                return False
            return user['subscription_ends'] > datetime.utcnow()
    
    async def process_mailing(self, task):
        task_id = task['id']
        user_id = task['user_id']
        
        print(f"📨 Processing mailing #{task_id}")
        
        if not await self.check_subscription(user_id):
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE mailing_task SET status = 'Ошибка: нет подписки' WHERE id = $1", task_id)
            return
        
        async with db_pool.acquire() as conn:
            account = await conn.fetchrow('SELECT * FROM account WHERE id = $1', task['account_id'])
        
        if not account or not account['session_string']:
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE mailing_task SET status = 'Ошибка: аккаунт не найден' WHERE id = $1", task_id)
            return
        
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE mailing_task SET status = 'В работе' WHERE id = $1", task_id)
        
        self.running_mailings[task_id] = True
        
        try:
            client = TelegramClient(StringSession(account['session_string']), API_ID, API_HASH)
            await client.connect()
            
            if not await client.is_user_authorized():
                async with db_pool.acquire() as conn:
                    await conn.execute("UPDATE mailing_task SET status = 'Ошибка: сессия недействительна' WHERE id = $1", task_id)
                await client.disconnect()
                del self.running_mailings[task_id]
                return
            
            chats = json.loads(task['chats'])
            message = task['message']
            delay = task['delay']
            mailing_type = task['mailing_type']
            
            if mailing_type == 'random':
                import random
                random.shuffle(chats)
            
            sent = task['sent_count']
            total = len(chats)
            
            for i in range(sent, total):
                if not self.running_mailings.get(task_id):
                    print(f"⏸ Mailing #{task_id} paused")
                    await client.disconnect()
                    return
                
                chat_id = chats[i]
                
                try:
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            'UPDATE mailing_task SET current_chat = $1 WHERE id = $2',
                            str(chat_id), task_id
                        )
                    
                    entity = await client.get_entity(int(chat_id))
                    await client.send_message(entity, message)
                    sent += 1
                    
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            'UPDATE mailing_task SET sent_count = $1, current_chat = NULL WHERE id = $2',
                            sent, task_id
                        )
                    
                    print(f"  ✅ [{sent}/{total}] Sent to {chat_id}")
                    
                    if delay > 0 and sent < total:
                        await asyncio.sleep(delay)
                        
                except FloodWaitError as e:
                    print(f"  ⏳ Flood wait {e.seconds}s")
                    await asyncio.sleep(e.seconds)
                    try:
                        entity = await client.get_entity(int(chat_id))
                        await client.send_message(entity, message)
                        sent += 1
                        async with db_pool.acquire() as conn:
                            await conn.execute('UPDATE mailing_task SET sent_count = $1 WHERE id = $2', sent, task_id)
                    except:
                        pass
                except ChatWriteForbiddenError:
                    print(f"  ⚠️ No access to {chat_id}")
                except Exception as e:
                    print(f"  ❌ Error in {chat_id}: {e}")
            
            async with db_pool.acquire() as conn:
                await conn.execute("UPDATE mailing_task SET status = 'Завершено' WHERE id = $1", task_id)
            
            await client.disconnect()
            print(f"✅ Mailing #{task_id} completed ({sent}/{total})")
            
        except Exception as e:
            async with db_pool.acquire() as conn:
                await conn.execute(f"UPDATE mailing_task SET status = 'Ошибка' WHERE id = $1", task_id)
            print(f"❌ Critical error in mailing #{task_id}: {e}")
        
        if task_id in self.running_mailings:
            del self.running_mailings[task_id]
    
    async def setup_autoresponder(self, responder, account, user_id):
        if not await self.check_subscription(user_id):
            return
        
        responder_id = responder['id']
        
        if responder_id in self.active_autoresponders:
            try:
                await self.active_autoresponders[responder_id]['client'].disconnect()
            except:
                pass
            del self.active_autoresponders[responder_id]
        
        if not responder['is_active']:
            return
        
        try:
            client = TelegramClient(StringSession(account['session_string']), API_ID, API_HASH)
            await client.connect()
            
            if not await client.is_user_authorized():
                await client.disconnect()
                return
            
            trigger_type = responder['trigger_type']
            keywords = [k.strip().lower() for k in responder['keywords'].split(',')] if responder['keywords'] else ['-']
            reply_text = responder['reply_text']
            
            @client.on(events.NewMessage(incoming=True))
            async def handler(event):
                try:
                    if trigger_type == 'pms' and not event.is_private:
                        return
                    if trigger_type == 'groups' and not (event.is_group or event.is_channel):
                        return
                    
                    if keywords != ['-']:
                        message_text = event.message.text.lower() if event.message.text else ''
                        if not any(kw in message_text for kw in keywords):
                            return
                    
                    await event.reply(reply_text)
                    
                    async with db_pool.acquire() as conn:
                        await conn.execute(
                            'UPDATE autoresponder SET response_count = COALESCE(response_count, 0) + 1, last_response = NOW() WHERE id = $1',
                            responder_id
                        )
                    
                    print(f"  🤖 Auto-reply to {event.sender_id}")
                    
                except Exception as e:
                    print(f"  ⚠️ Autoresponder error: {e}")
            
            self.active_autoresponders[responder_id] = {
                'client': client,
                'handler': handler
            }
            
            print(f"🤖 Autoresponder #{responder_id} activated")
            
        except Exception as e:
            print(f"❌ Error setting up autoresponder #{responder_id}: {e}")
    
    async def check_mailing_tasks(self):
        async with db_pool.acquire() as conn:
            tasks = await conn.fetch("SELECT * FROM mailing_task WHERE status = 'Ожидает' LIMIT 5")
            for task in tasks:
                if task['id'] not in self.running_mailings:
                    asyncio.create_task(self.process_mailing(task))
    
    async def check_paused_tasks(self):
        async with db_pool.acquire() as conn:
            tasks = await conn.fetch("SELECT * FROM mailing_task WHERE status = 'Остановлено'")
            for task in tasks:
                if task['id'] in self.running_mailings:
                    self.running_mailings[task['id']] = False
    
    async def check_autoresponders(self):
        async with db_pool.acquire() as conn:
            responders = await conn.fetch('SELECT * FROM autoresponder WHERE is_active = true')
            
            for responder in responders:
                user_id = responder['user_id']
                
                if not await self.check_subscription(user_id):
                    if responder['id'] in self.active_autoresponders:
                        try:
                            await self.active_autoresponders[responder['id']]['client'].disconnect()
                        except:
                            pass
                        del self.active_autoresponders[responder['id']]
                    continue
                
                if responder['id'] not in self.active_autoresponders:
                    account = await conn.fetchrow('SELECT * FROM account WHERE id = $1', responder['account_id'])
                    if account and account['session_string']:
                        await self.setup_autoresponder(responder, account, user_id)
    
    async def main_loop(self):
        print("🔄 Vest Traffer Worker started")
        
        while True:
            try:
                await self.check_mailing_tasks()
                await self.check_paused_tasks()
                await self.check_autoresponders()
            except Exception as e:
                print(f"⚠️ Main loop error: {e}")
            
            await asyncio.sleep(5)

@dp.message(Command("start"))
async def cmd_start(message: types.Message):
    keyboard = types.InlineKeyboardMarkup(inline_keyboard=[
        [types.InlineKeyboardButton(text="Open Panel", url="https://your-site.vercel.app")]
    ])
    
    await message.answer(
        "Welcome to Vest Traffer!\n\n"
        "• Telegram mass mailing\n"
        "• Auto-responders\n"
        "• Account management\n\n"
        "Click below to open control panel:",
        reply_markup=keyboard
    )

async def main():
    worker = VestTrafferWorker()
    await worker.init_db()
    print("✅ Database initialized")
    
    asyncio.create_task(worker.main_loop())
    
    print("🤖 Bot started")
    await dp.start_polling(bot)

if __name__ == '__main__':
    try:
        print("🟢 Starting Vest Traffer System...")
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n🛑 Shutting down...")
    except Exception as e:
        print(f"❌ Critical error: {e}")
