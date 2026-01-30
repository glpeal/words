"""
Пример использования TikTok Parser Module

Это полноценный пример бота с интегрированным парсером TikTok.
Вы можете использовать этот код как основу или взять только нужные части.
"""

import asyncio
import logging
from aiogram import Bot, Dispatcher, F
from aiogram.types import Message
from aiogram.filters import Command
from aiogram.fsm.storage.memory import MemoryStorage

# Импорт модуля парсера
from tiktok_parser import TikTokParserModule

# Конфигурация
BOT_TOKEN = "YOUR_BOT_TOKEN"
STORAGE_CHAT_ID = -1001234567890  # ID группы для хранения видео

# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def main():
    # Инициализация бота
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    # ============================================
    # СПОСОБ 1: Полная интеграция модуля (рекомендуется)
    # ============================================

    # Создаем экземпляр модуля
    tiktok_module = TikTokParserModule(
        bot=bot,
        storage_chat_id=STORAGE_CHAT_ID,
        command="tiktok",           # Команда /tiktok
        max_videos=20,              # Максимум 20 видео за раз
        default_count=5,            # По умолчанию 5 видео
        admin_ids=None,             # None = доступ всем, или [123456789] для ограничения
        # Опциональные колбэки для статистики
        on_start_callback=lambda user_id, query, count: logger.info(f"User {user_id} searching: {query} ({count})"),
        on_complete_callback=lambda user_id, count: logger.info(f"User {user_id} received {count} videos")
    )

    # Регистрируем хэндлеры модуля
    tiktok_module.register_handlers(dp)

    # ============================================
    # Ваши собственные хэндлеры
    # ============================================

    @dp.message(Command("start"))
    async def cmd_start(message: Message):
        await message.answer(
            "👋 Привет!\n\n"
            "Я бот для скачивания видео с TikTok.\n\n"
            "**Команды:**\n"
            "/tiktok - Поиск видео по запросу\n"
            "/help - Помощь\n\n"
            "Также можете просто отправить ссылку на TikTok видео!",
            parse_mode="Markdown"
        )

    @dp.message(Command("help"))
    async def cmd_help(message: Message):
        await message.answer(
            "**Как пользоваться:**\n\n"
            "1️⃣ Введите /tiktok\n"
            "2️⃣ Напишите запрос (например: смешные коты)\n"
            "3️⃣ Выберите количество видео\n"
            "4️⃣ Получите видео!\n\n"
            "**Или:**\n"
            "Просто отправьте ссылку на TikTok видео,\n"
            "и я скачаю его без водяного знака.",
            parse_mode="Markdown"
        )

    # Запуск
    logger.info("Bot starting...")

    try:
        await dp.start_polling(bot)
    finally:
        await tiktok_module.close()
        await bot.session.close()


if __name__ == "__main__":
    asyncio.run(main())
