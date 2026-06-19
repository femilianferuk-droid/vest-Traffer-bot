# bot.py
import os
import asyncio
import json
import random
import psycopg2
from psycopg2.extras import RealDictCursor
from aiogram import Bot, Dispatcher
from telethon import TelegramClient, events
from telethon.sessions import StringSession

BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")

API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Глобальный реестр активных задач автоответчиков: {account_id: {"client": client, "config_id": id}}
active_autoresponders = {}

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

# --- Обработчик Задач на Массовую Рассылку ---
async def execute_mailing_task(task):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("UPDATE mailing_task SET status = 'В работе' WHERE id = %s", (task['id'],))
    conn.commit()
    
    cursor.execute("SELECT session_string FROM account WHERE id = %s", (task['account_id'],))
    account = cursor.fetchone()
    
    if not account:
        cursor.execute("UPDATE mailing_task SET status = 'Ошибка: сессия удалена' WHERE id = %s", (task['id'],))
        conn.commit()
        cursor.close()
        conn.close()
        return

    client = TelegramClient(StringSession(account['session_string']), API_ID, API_HASH)
    try:
        await client.connect()
        chat_ids = json.loads(task['chats'])
        message_text = task['message']
        delay = task['delay']
        total_messages = task['total_messages']
        mailing_type = task['mailing_type']
        
        sent_count = 0
        
        if mailing_type == 'simultaneous':
            while sent_count < total_messages:
                for cid in chat_ids:
                    if sent_count >= total_messages:
                        break
                    try:
                        await client.send_message(int(cid), message_text)
                        sent_count += 1
                        
                        # Обновляем счетчик для прогресс-бара на сайте
                        cursor.execute("UPDATE mailing_task SET sent_count = %s WHERE id = %s", (sent_count, task['id']))
                        conn.commit()
                        
                        await asyncio.sleep(delay)
                    except Exception as e:
                        print(f"Ошибка отправки в чат {cid}: {e}")
                        await asyncio.sleep(2)
                        
        elif mailing_type == 'random':
            for _ in range(total_messages):
                target_chat = random.choice(chat_ids)
                try:
                    await client.send_message(int(target_chat), message_text)
                    sent_count += 1
                    cursor.execute("UPDATE mailing_task SET sent_count = %s WHERE id = %s", (sent_count, task['id']))
                    conn.commit()
                except Exception as e:
                    print(f"Ошибка отправки в случайный чат {target_chat}: {e}")
                await asyncio.sleep(delay)

        cursor.execute("UPDATE mailing_task SET status = 'Завершено' WHERE id = %s", (task['id'],))
        conn.commit()
    except Exception as main_err:
        cursor.execute("UPDATE mailing_task SET status = %s WHERE id = %s", (f"Ошибка: {str(main_err)}", task['id']))
        conn.commit()
    finally:
        await client.disconnect()
        cursor.close()
        conn.close()

# --- Динамический хэндлер триггеров Автоответчика ---
def build_autoresponder_handler(config):
    async def reply_handler(event):
        if config['trigger_type'] == 'pms' and not event.is_private:
            return
        if config['trigger_type'] == 'groups' and not event.is_group:
            return
            
        sender = await event.get_sender()
        if sender and (sender.bot or sender.is_self):
            return
            
        raw_text = event.raw_text.lower() if event.raw_text else ""
        keywords = config['keywords'].strip()
        
        is_triggered = False
        if keywords == '-':
            is_triggered = True
        else:
            kw_list = [k.strip().lower() for k in keywords.split(',') if k.strip()]
            if any(kw in raw_text for kw in kw_list):
                is_triggered = True
                
        if is_triggered:
            try:
                await event.reply(config['reply_text'])
                print(f"[Autoresponder] Успешный автоответ в чат {event.chat_id}")
            except Exception as reply_err:
                print(f"Ошибка автоответа: {reply_err}")
                
    return reply_handler

# --- Фоновый Воркер (Бесконечный Поллинг Базы) ---
async def database_polling_worker():
    print("[Worker] Запуск фонового мониторинга Vest Traffer...")
    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 1. Поиск задач на массовую рассылку
            cursor.execute("SELECT * FROM mailing_task WHERE status = 'Ожидает' LIMIT 1")
            task = cursor.fetchone()
            if task:
                asyncio.create_task(execute_mailing_task(task))
                
            # 2. Синхронизация состояний Автоответчиков
            cursor.execute("SELECT autoresponder.*, account.session_string FROM autoresponder JOIN account ON autoresponder.account_id = account.id WHERE autoresponder.is_active = True")
            db_responders = cursor.fetchall()
            db_account_ids = [r['account_id'] for r in db_responders]
            
            # Отключение деактивированных или удаленных на сайте автоответчиков
            for active_id in list(active_autoresponders.keys()):
                if active_id not in db_account_ids:
                    print(f"[Autoresponder] Отключение слушателя для аккаунта ID {active_id}")
                    try:
                        await active_autoresponders[active_id]['client'].disconnect()
                    except:
                        pass
                    active_autoresponders.pop(active_id)
                    
            # Инициализация и поднятие новых автоответчиков в реальном времени
            for r in db_responders:
                acc_id = r['account_id']
                if acc_id not in active_autoresponders:
                    print(f"[Autoresponder] Запуск нового слушателя для аккаунта ID {acc_id}")
                    cl = TelegramClient(StringSession(r['session_string']), API_ID, API_HASH)
                    try:
                        await cl.connect()
                        handler = build_autoresponder_handler(r)
                        cl.add_event_handler(handler, events.NewMessage(incoming=True))
                        
                        active_autoresponders[acc_id] = {
                            'client': cl,
                            'config_id': r['id']
                        }
                    except Exception as cl_err:
                        print(f"Ошибка старта сессии {acc_id}: {cl_err}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"[Worker Error] Сбой синхронизации: {e}")
            
        await asyncio.sleep(5)

async def main():
    asyncio.create_task(database_polling_worker())
    print("[Bot] Сервис aiogram запущен.")
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
