# bot.py
import os
import asyncio
import json
import random
import datetime
import psycopg2
from psycopg2.extras import RealDictCursor
from aiogram import Bot, Dispatcher
from telethon import TelegramClient, events
from telethon.sessions import StringSession

# Считывание токена и URL базы данных из переменных окружения (всё по ТЗ)
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")

API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

# Глобальный реестр запущенных процессов автоответчиков: {account_id: {"client": client, "config_id": id}}
active_autoresponders = {}

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

def check_user_subscription(cursor, user_id):
    """Проверка активности тарифа пользователя (включая тестовый период)"""
    cursor.execute("SELECT subscription_ends FROM users WHERE id = %s", (user_id,))
    res = cursor.fetchone()
    if res and res['subscription_ends']:
        return res['subscription_ends'] > datetime.datetime.utcnow()
    return False

# --- Функция Исполнения Кампании Массовой Рассылки ---
async def execute_mailing_task(task):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Валидация подписки перед фактическим стартом отправки сообщений
    if not check_user_subscription(cursor, task['user_id']):
        cursor.execute("UPDATE mailing_task SET status = 'Ошибка: нет подписки' WHERE id = %s", (task['id'],))
        conn.commit()
        cursor.close()
        conn.close()
        return
        
    cursor.execute("UPDATE mailing_task SET status = 'В работе' WHERE id = %s", (task['id'],))
    conn.commit()
    
    cursor.execute("SELECT session_string FROM account WHERE id = %s", (task['account_id'],))
    account = cursor.fetchone()
    
    if not account:
        cursor.execute("UPDATE mailing_task SET status = 'Ошибка: сессия не найдена' WHERE id = %s", (task['id'],))
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
            # Одновременный (круговой) обход выбранного пула чатов и ЛС
            while sent_count < total_messages:
                for cid in chat_ids:
                    if sent_count >= total_messages:
                        break
                    try:
                        await client.send_message(int(cid), message_text)
                        sent_count += 1
                        
                        # Обновление счетчика для прогресс-бара в веб-интерфейсе
                        cursor.execute("UPDATE mailing_task SET sent_count = %s WHERE id = %s", (sent_count, task['id']))
                        conn.commit()
                        
                        await asyncio.sleep(delay)
                    except Exception as e:
                        print(f"Ошибка отправки софтом Vest Traffer в чат {cid}: {e}")
                        await asyncio.sleep(2)
                        
        elif mailing_type == 'random':
            # Рандомный (выборочный) обход целей из пула
            for _ in range(total_messages):
                target_chat = random.choice(chat_ids)
                try:
                    await client.send_message(int(target_chat), message_text)
                    sent_count += 1
                    
                    cursor.execute("UPDATE mailing_task SET sent_count = %s WHERE id = %s", (sent_count, task['id']))
                    conn.commit()
                except Exception as e:
                    print(f"Ошибка отправки в случайный таргет {target_chat}: {e}")
                    
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

# --- Динамический Конструктор Триггеров Автоответчика ---
def build_autoresponder_handler(config):
    async def reply_handler(event):
        # Фильтрация по типу входящей области (Только ЛС / Только группы / Все сообщения)
        if config['trigger_type'] == 'pms' and not event.is_private:
            return
        if config['trigger_type'] == 'groups' and not event.is_group:
            return
            
        # Защита от зацикливания ответов автореспондера на самого себя или других ботов
        sender = await event.get_sender()
        if sender and (sender.bot or sender.is_self):
            return
            
        raw_text = event.raw_text.lower() if event.raw_text else ""
        keywords = config['keywords'].strip()
        
        is_triggered = False
        # Знак "-" означает тотальный автоответ на абсолютно любое входящее сообщение
        if keywords == '-':
            is_triggered = True
        else:
            # Парсинг ключевых слов через запятую
            kw_list = [k.strip().lower() for k in keywords.split(',') if k.strip()]
            if any(kw in raw_text for kw in kw_list):
                is_triggered = True
                
        if is_triggered:
            try:
                await event.reply(config['reply_text'])
                print(f"[Autoresponder] Отправлен автоответ для аккаунта ID {config['account_id']} в чат {event.chat_id}")
            except Exception as reply_err:
                print(f"Ошибка отправки автоответа клиентом Telethon: {reply_err}")
                
    return reply_handler

# --- Фоновый Асинхронный Воркер Vest Traffer (Бесконечный Цикл) ---
async def database_polling_worker():
    print("[Worker] Фоновые службы автоматизации Vest Traffer успешно запущены.")
    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # 1. Поиск задач на массовую циклическую / рандомную рассылку
            cursor.execute("SELECT * FROM mailing_task WHERE status = 'Ожидает' LIMIT 1")
            task = cursor.fetchone()
            if task:
                asyncio.create_task(execute_mailing_task(task))
                
            # 2. Синхронизация и удержание сессий Умного Автоответчика
            # Выбираются автоответчики только тех пользователей, у которых на текущий момент подписка активна
            cursor.execute(\
                "SELECT autoresponder.*, account.session_string FROM autoresponder "
                "JOIN account ON autoresponder.account_id = account.id "
                "JOIN users ON autoresponder.user_id = users.id "
                "WHERE autoresponder.is_active = True AND users.subscription_ends > NOW()"\
            )
            db_responders = cursor.fetchall()
            db_account_ids = [r['account_id'] for r in db_responders]
            
            # Принудительное отключение слушателей, если подписка истекла или автоответчик удален на сайте
            for active_id in list(active_autoresponders.keys()):
                if active_id not in db_account_ids:
                    print(f"[Autoresponder] Деактивация и отключение слушателя для аккаунта ID {active_id}")
                    try:
                        await active_autoresponders[active_id]['client'].disconnect()
                    except:
                        pass
                    active_autoresponders.pop(active_id)
                    
            # Инициализация и запуск новых сессий автоответа в фоновом режиме
            for r in db_responders:
                acc_id = r['account_id']
                if acc_id not in active_autoresponders:
                    print(f"[Autoresponder] Активация и подключение сессии автоответа для аккаунта ID {acc_id}")
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
                        print(f"Не удалось поднять фоновый автоответчик для сессии {acc_id}: {cl_err}")
            
            cursor.close()
            conn.close()
            
        except Exception as e:
            print(f"[Worker Error] Критическая ошибка итерации поллинга базы данных: {e}")
            
        # Интервал опроса базы данных — 5 секунд (согласно ТЗ)
        await asyncio.sleep(5)

async def main():
    # Запуск бесконечного цикла воркера параллельно с aiogram-поллингом
    asyncio.create_task(database_polling_worker())
    print("[Bot] Диспетчер aiogram успешно активирован.")
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
