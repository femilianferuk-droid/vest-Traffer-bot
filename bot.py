# bot.py
import os
import asyncio
import json
import random
import psycopg2
from psycopg2.extras import RealDictCursor
from aiogram import Bot, Dispatcher
from telethon import TelegramClient
from telethon.sessions import StringSession

# Считываем конфигурацию из переменных окружения (всё по ТЗ)
BOT_TOKEN = os.environ.get("BOT_TOKEN")
DATABASE_URL = os.environ.get("DATABASE_URL")

API_ID = 32480523
API_HASH = "147839735c9fa4e83451209e9b55cfc5"

bot = Bot(token=BOT_TOKEN)
dp = Dispatcher()

def get_db_connection():
    return psycopg2.connect(DATABASE_URL, cursor_factory=RealDictCursor)

async def execute_mailing_task(task):
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Меняем статус в БД на "В работе"
    cursor.execute("UPDATE mailing_task SET status = 'В работе' WHERE id = %s", (task['id'],))
    conn.commit()
    
    # Достаем строковую сессию аккаунта-исполнителя
    cursor.execute("SELECT session_string, phone FROM account WHERE id = %s", (task['account_id'],))
    account = cursor.fetchone()
    
    if not account:
        cursor.execute("UPDATE mailing_task SET status = 'Ошибка: Аккаунт не найден' WHERE id = %s", (task['id'],))
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
            # Одновременный тип: идем по кругу чатов, пока не отправим нужное общее кол-во сообщений
            while sent_count < total_messages:
                for cid in chat_ids:
                    if sent_count >= total_messages:
                        break
                    try:
                        await client.send_message(int(cid), message_text)
                        sent_count += 1
                        await asyncio.sleep(delay)
                    except Exception as e:
                        print(f"Ошибка отправки в чат {cid}: {e}")
                        await asyncio.sleep(1) # Небольшая пауза при ошибке, чтобы обходить флуд-вейт
                        
        elif mailing_type == 'random':
            # Рандомный тип: выбираем каждый раз случайный чат из пула
            for _ in range(total_messages):
                target_chat = random.choice(chat_ids)
                try:
                    await client.send_message(int(target_chat), message_text)
                    sent_count += 1
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

# Фоновый обработчик очереди задач из БД
async def database_polling_worker():
    print("[Worker] Фоновый воркер рассылок успешно запущен.")
    while True:
        try:
            conn = get_db_connection()
            cursor = conn.cursor()
            
            # Ищем одну задачу со статусом "Ожидает"
            cursor.execute("SELECT * FROM mailing_task WHERE status = 'Ожидает' LIMIT 1")
            task = cursor.fetchone()
            
            cursor.close()
            conn.close()
            
            if task:
                print(f"[Worker] Найдена активная задача #{task['id']}. Запуск...")
                # Запускаем выполнение задачи асинхронно, чтобы не блокировать цикл проверки новых задач
                asyncio.create_task(execute_mailing_task(task))
                
        except Exception as e:
            print(f"[Worker] Ошибка при чтении из базы данных: {e}")
            
        await asyncio.sleep(5) # Проверяем базу данных каждые 5 секунд

async def main():
    # Запускаем фоновый поллинг базы данных параллельно с ботом
    asyncio.create_task(database_polling_worker())
    
    print("[Bot] Запуск Telegram API интерфейса бота...")
    await dp.start_polling(bot)

if __name__ == '__main__':
    asyncio.run(main())
