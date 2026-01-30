"""
Примеры интеграции TikTok Parser в существующего бота

Здесь показаны разные способы интеграции модуля в ваш бот.
Выберите подходящий вариант в зависимости от структуры вашего бота.
"""

# ============================================
# ВАРИАНТ 1: Простая интеграция в aiogram 3.x
# ============================================

"""
# В вашем main.py или bot.py:

from aiogram import Bot, Dispatcher
from tiktok_parser import TikTokParserModule

bot = Bot(token=TOKEN)
dp = Dispatcher()

# Создаем модуль и регистрируем
tiktok = TikTokParserModule(
    bot=bot,
    storage_chat_id=-1001234567890  # Ваша группа для хранения
)
tiktok.register_handlers(dp)

# Или через роутер
# dp.include_router(tiktok.get_router())
"""


# ============================================
# ВАРИАНТ 2: Интеграция с существующими роутерами
# ============================================

"""
# Если у вас уже есть структура с роутерами:

from aiogram import Router
from tiktok_parser import TikTokParserModule

# Ваш основной роутер
main_router = Router()

# В функции setup или init:
def setup_tiktok(bot, dp):
    tiktok = TikTokParserModule(
        bot=bot,
        storage_chat_id=STORAGE_CHAT_ID,
        command="tt"  # Можно изменить команду
    )
    dp.include_router(tiktok.get_router())
    return tiktok
"""


# ============================================
# ВАРИАНТ 3: Простой хэндлер без FSM
# ============================================

"""
# Если не хотите использовать FSM:

from tiktok_parser import TikTokParserModule
from tiktok_parser.module import SimpleTikTokHandler

handler = SimpleTikTokHandler(bot, storage_chat_id=-1001234567890)

@dp.message(Command("tt"))
async def tt_command(message: Message):
    # Формат: /tt запрос количество
    # Пример: /tt смешные коты 5
    await handler.handle_command(message, default_count=5)

@dp.message(F.text.contains("tiktok.com"))
async def tiktok_link(message: Message):
    await handler.handle_url(message)
"""


# ============================================
# ВАРИАНТ 4: Использование только парсера
# ============================================

"""
# Если хотите полный контроль над логикой:

from tiktok_parser import TikTokParser

parser = TikTokParser(bot, storage_chat_id=-1001234567890)

@dp.message(Command("search"))
async def search_videos(message: Message):
    query = message.text.split(maxsplit=1)[1] if len(message.text.split()) > 1 else None
    if not query:
        await message.answer("Укажите запрос: /search котики")
        return

    # Получаем видео
    videos = await parser.search_and_parse(query, count=5)

    # Отправляем пользователю
    for video in videos:
        await parser.send_video_to_user(message.chat.id, video)
"""


# ============================================
# ВАРИАНТ 5: Только скачивание по ссылке
# ============================================

"""
# Если нужно только скачивать по ссылкам:

from tiktok_parser import TikTokDownloader

downloader = TikTokDownloader()

@dp.message(F.text.regexp(r'tiktok\.com'))
async def download_tiktok(message: Message):
    # Получаем инфо о видео
    info = await downloader.get_video_info(message.text)
    if not info:
        await message.answer("Не удалось получить видео")
        return

    # Скачиваем
    video_bytes = await downloader.download_video(info.video_url)
    if not video_bytes:
        await message.answer("Не удалось скачать видео")
        return

    # Отправляем
    await message.answer_video(
        video=video_bytes,
        caption=f"🎬 {info.author}\n{info.title[:200]}"
    )
"""


# ============================================
# ВАРИАНТ 6: Интеграция с базой данных
# ============================================

"""
# Пример с сохранением статистики в БД:

from tiktok_parser import TikTokParserModule

async def on_search_start(user_id: int, query: str, count: int):
    # Сохраняем в БД
    await db.log_search(user_id, query, count)

async def on_search_complete(user_id: int, videos_count: int):
    # Обновляем статистику
    await db.increment_videos_sent(user_id, videos_count)

tiktok = TikTokParserModule(
    bot=bot,
    storage_chat_id=STORAGE_CHAT_ID,
    on_start_callback=on_search_start,
    on_complete_callback=on_search_complete
)
"""


# ============================================
# ВАРИАНТ 7: С ограничением доступа
# ============================================

"""
# Ограничение только для определенных пользователей:

ALLOWED_USERS = [123456789, 987654321]  # Telegram user IDs

tiktok = TikTokParserModule(
    bot=bot,
    storage_chat_id=STORAGE_CHAT_ID,
    admin_ids=ALLOWED_USERS  # Только эти пользователи смогут использовать
)
"""


# ============================================
# Пример полной интеграции
# ============================================

async def example_full_integration():
    """
    Полный пример интеграции в существующего бота
    """
    from aiogram import Bot, Dispatcher, F
    from aiogram.types import Message
    from aiogram.filters import Command
    from aiogram.fsm.storage.memory import MemoryStorage

    from tiktok_parser import TikTokParserModule

    # Ваш существующий код бота
    BOT_TOKEN = "your_token"
    STORAGE_CHAT_ID = -1001234567890

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    # === Ваши существующие хэндлеры ===

    @dp.message(Command("start"))
    async def start(message: Message):
        await message.answer("Привет!")

    @dp.message(Command("mycommand"))
    async def my_command(message: Message):
        await message.answer("Ваша команда")

    # === Добавляем TikTok модуль ===

    tiktok = TikTokParserModule(
        bot=bot,
        storage_chat_id=STORAGE_CHAT_ID,
        command="tiktok"  # /tiktok
    )
    tiktok.register_handlers(dp)

    # === Запуск ===

    try:
        await dp.start_polling(bot)
    finally:
        await tiktok.close()


if __name__ == "__main__":
    import asyncio
    asyncio.run(example_full_integration())
