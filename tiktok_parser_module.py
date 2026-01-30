"""
TikTok Parser Module for Telegram Bot
=====================================

Модуль для парсинга и скачивания видео с TikTok без водяного знака.
Видео хранятся в Telegram группе, не занимая место локально.

Требования:
    pip install aiogram aiohttp

Использование:
    1. Создайте приватную группу в Telegram
    2. Добавьте бота с правами админа
    3. Получите ID группы через @userinfobot

    from tiktok_parser_module import TikTokParserModule

    tiktok = TikTokParserModule(
        bot=bot,
        storage_chat_id=-1001234567890
    )
    tiktok.register_handlers(dp)

Автор: Claude
"""

import asyncio
import re
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO

import aiohttp
from aiogram import Router, F, Bot
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder


# ============================================
# КОНФИГУРАЦИЯ (измените под себя)
# ============================================

class Config:
    """Настройки модуля"""
    # ID группы для хранения видео (получить через @userinfobot)
    STORAGE_CHAT_ID: int = -1001234567890

    # Команда для запуска парсера
    COMMAND: str = "tiktok"

    # Максимальное количество видео за запрос
    MAX_VIDEOS: int = 20

    # Количество по умолчанию
    DEFAULT_COUNT: int = 5

    # Максимальная длительность видео (секунды)
    MAX_DURATION: int = 180

    # Список ID админов (None = доступ всем)
    ADMIN_IDS: Optional[List[int]] = None

    # Таймаут HTTP запросов
    REQUEST_TIMEOUT: int = 30

    # Размер кэша
    MAX_CACHE_SIZE: int = 1000


# ============================================
# DATA CLASSES
# ============================================

@dataclass
class VideoInfo:
    """Информация о видео"""
    video_url: str
    video_url_watermark: str
    author: str
    author_id: str
    title: str
    cover_url: str
    music_title: str
    play_count: int
    like_count: int
    comment_count: int
    share_count: int
    duration: int
    original_url: str


@dataclass
class CachedVideo:
    """Кэшированное видео"""
    file_id: str
    video_url: str
    author: str
    title: str
    cached_at: datetime = field(default_factory=datetime.now)
    play_count: int = 0
    like_count: int = 0


# ============================================
# FSM STATES
# ============================================

class TikTokStates(StatesGroup):
    """FSM состояния для парсера"""
    waiting_for_query = State()
    waiting_for_count = State()
    processing = State()


# ============================================
# DOWNLOADER
# ============================================

class TikTokDownloader:
    """
    Скачивает видео с TikTok без водяного знака
    Использует tikwm.com API
    """

    TIKWM_API = "https://www.tikwm.com/api/"
    TIKWM_FEED_API = "https://www.tikwm.com/api/feed/search"

    def __init__(self, timeout: int = 30):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            )
        return self._session

    async def close(self):
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_video_info(self, url: str) -> Optional[VideoInfo]:
        """Получить информацию о видео по URL"""
        session = await self._get_session()

        try:
            async with session.post(self.TIKWM_API, data={"url": url, "hd": 1}) as response:
                if response.status != 200:
                    return None

                data = await response.json()
                if data.get("code") != 0:
                    return None

                v = data.get("data", {})
                return VideoInfo(
                    video_url=v.get("play", ""),
                    video_url_watermark=v.get("wmplay", ""),
                    author=v.get("author", {}).get("nickname", "Unknown"),
                    author_id=v.get("author", {}).get("unique_id", ""),
                    title=v.get("title", ""),
                    cover_url=v.get("cover", ""),
                    music_title=v.get("music_info", {}).get("title", ""),
                    play_count=v.get("play_count", 0),
                    like_count=v.get("digg_count", 0),
                    comment_count=v.get("comment_count", 0),
                    share_count=v.get("share_count", 0),
                    duration=v.get("duration", 0),
                    original_url=url
                )
        except Exception as e:
            print(f"Error getting video info: {e}")
            return None

    async def search_videos(self, query: str, count: int = 10, cursor: int = 0) -> List[Dict[str, Any]]:
        """Поиск видео по ключевым словам/хэштегам"""
        session = await self._get_session()
        query = query.lstrip('#')
        videos = []
        remaining = count

        while remaining > 0:
            batch_count = min(remaining, 30)

            try:
                async with session.post(
                    self.TIKWM_FEED_API,
                    data={"keywords": query, "count": batch_count, "cursor": cursor, "hd": 1}
                ) as response:
                    if response.status != 200:
                        break

                    data = await response.json()
                    if data.get("code") != 0:
                        break

                    batch_videos = data.get("data", {}).get("videos", [])
                    if not batch_videos:
                        break

                    for v in batch_videos:
                        videos.append({
                            "video_url": v.get("play", ""),
                            "video_url_watermark": v.get("wmplay", ""),
                            "author": v.get("author", {}).get("nickname", "Unknown"),
                            "author_id": v.get("author", {}).get("unique_id", ""),
                            "title": v.get("title", ""),
                            "cover_url": v.get("cover", ""),
                            "play_count": v.get("play_count", 0),
                            "like_count": v.get("digg_count", 0),
                            "comment_count": v.get("comment_count", 0),
                            "share_count": v.get("share_count", 0),
                            "duration": v.get("duration", 0),
                            "video_id": v.get("video_id", ""),
                            "region": v.get("region", "")
                        })

                    remaining -= len(batch_videos)
                    cursor = data.get("data", {}).get("cursor", cursor + batch_count)

                    if not data.get("data", {}).get("hasMore", False):
                        break

                    await asyncio.sleep(0.5)

            except Exception as e:
                print(f"Error searching videos: {e}")
                break

        return videos[:count]

    async def download_video(self, video_url: str) -> Optional[BytesIO]:
        """Скачать видео в память"""
        session = await self._get_session()

        try:
            async with session.get(video_url) as response:
                if response.status != 200:
                    return None

                video_bytes = await response.read()
                video_io = BytesIO(video_bytes)
                video_io.name = "video.mp4"
                return video_io

        except Exception as e:
            print(f"Error downloading video: {e}")
            return None


# ============================================
# PARSER
# ============================================

class TikTokParser:
    """Парсер с кэшированием в Telegram группе"""

    def __init__(self, bot: Bot, storage_chat_id: int, max_video_duration: int = 180, max_cache_size: int = 1000):
        self.bot = bot
        self.storage_chat_id = storage_chat_id
        self.max_video_duration = max_video_duration
        self.max_cache_size = max_cache_size
        self.downloader = TikTokDownloader()
        self._cache: Dict[str, CachedVideo] = {}

    async def close(self):
        await self.downloader.close()

    def _add_to_cache(self, video_id: str, cached: CachedVideo):
        if len(self._cache) >= self.max_cache_size:
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k].cached_at)
            del self._cache[oldest_key]
        self._cache[video_id] = cached

    async def search_and_parse(self, query: str, count: int = 5, progress_callback=None) -> List[Dict[str, Any]]:
        """Найти и подготовить видео"""
        videos = await self.downloader.search_videos(query, count=count)
        if not videos:
            return []

        results = []

        for i, video in enumerate(videos):
            video_id = video.get("video_id", "")
            duration = video.get("duration", 0)

            if duration > self.max_video_duration:
                continue

            # Проверяем кэш
            if video_id in self._cache:
                cached = self._cache[video_id]
                results.append({
                    "file_id": cached.file_id,
                    "author": cached.author,
                    "title": cached.title,
                    "from_cache": True,
                    **video
                })
                if progress_callback:
                    await progress_callback(i + 1, len(videos), video)
                continue

            # Скачиваем и загружаем в storage
            try:
                video_url = video.get("video_url", "")
                if not video_url:
                    continue

                video_io = await self.downloader.download_video(video_url)
                if not video_io:
                    continue

                caption = f"🎬 @{video.get('author_id', 'unknown')}\n{video.get('title', '')[:200]}"

                msg = await self.bot.send_video(
                    chat_id=self.storage_chat_id,
                    video=video_io,
                    caption=caption,
                    disable_notification=True
                )

                file_id = msg.video.file_id

                cached = CachedVideo(
                    file_id=file_id,
                    video_url=video_url,
                    author=video.get("author", ""),
                    title=video.get("title", ""),
                    play_count=video.get("play_count", 0),
                    like_count=video.get("like_count", 0)
                )
                self._add_to_cache(video_id, cached)

                results.append({"file_id": file_id, "from_cache": False, **video})

                if progress_callback:
                    await progress_callback(i + 1, len(videos), video)

                await asyncio.sleep(1)

            except Exception as e:
                print(f"Error processing video {video_id}: {e}")
                continue

        return results

    async def send_video_to_user(self, chat_id: int, video_data: Dict[str, Any], show_stats: bool = True) -> bool:
        """Отправить видео пользователю"""
        file_id = video_data.get("file_id")
        if not file_id:
            return False

        try:
            caption_parts = [f"🎬 **{video_data.get('author', 'Unknown')}**"]

            title = video_data.get("title", "")
            if title:
                caption_parts.append(f"\n{title[:300]}")

            if show_stats:
                stats = []
                if video_data.get("play_count"):
                    stats.append(f"👁 {self._format_number(video_data['play_count'])}")
                if video_data.get("like_count"):
                    stats.append(f"❤️ {self._format_number(video_data['like_count'])}")
                if video_data.get("comment_count"):
                    stats.append(f"💬 {self._format_number(video_data['comment_count'])}")
                if stats:
                    caption_parts.append(f"\n\n{' | '.join(stats)}")

            caption = "".join(caption_parts)

            await self.bot.send_video(chat_id=chat_id, video=file_id, caption=caption, parse_mode="Markdown")
            return True

        except Exception as e:
            print(f"Error sending video: {e}")
            return False

    @staticmethod
    def _format_number(num: int) -> str:
        if num >= 1_000_000:
            return f"{num / 1_000_000:.1f}M"
        elif num >= 1_000:
            return f"{num / 1_000:.1f}K"
        return str(num)

    async def get_video_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Получить видео по прямой TikTok ссылке"""
        info = await self.downloader.get_video_info(url)
        if not info:
            return None

        video_io = await self.downloader.download_video(info.video_url)
        if not video_io:
            return None

        try:
            caption = f"🎬 @{info.author_id}\n{info.title[:200]}"

            msg = await self.bot.send_video(
                chat_id=self.storage_chat_id,
                video=video_io,
                caption=caption,
                disable_notification=True
            )

            return {
                "file_id": msg.video.file_id,
                "author": info.author,
                "author_id": info.author_id,
                "title": info.title,
                "play_count": info.play_count,
                "like_count": info.like_count,
                "comment_count": info.comment_count,
                "duration": info.duration
            }

        except Exception as e:
            print(f"Error uploading video: {e}")
            return None


# ============================================
# MAIN MODULE
# ============================================

class TikTokParserModule:
    """
    Готовый модуль для интеграции в aiogram 3.x бота

    Пример:
        from tiktok_parser_module import TikTokParserModule

        tiktok = TikTokParserModule(
            bot=bot,
            storage_chat_id=-1001234567890
        )
        tiktok.register_handlers(dp)
    """

    def __init__(
        self,
        bot: Bot,
        storage_chat_id: int,
        command: str = "tiktok",
        max_videos: int = 20,
        default_count: int = 5,
        admin_ids: Optional[List[int]] = None,
        on_start_callback: Optional[Callable] = None,
        on_complete_callback: Optional[Callable] = None
    ):
        self.bot = bot
        self.storage_chat_id = storage_chat_id
        self.command = command
        self.max_videos = max_videos
        self.default_count = default_count
        self.admin_ids = admin_ids
        self.on_start_callback = on_start_callback
        self.on_complete_callback = on_complete_callback

        self.parser = TikTokParser(bot, storage_chat_id)
        self.router = Router(name="tiktok_parser")

        self._setup_handlers()

    def _setup_handlers(self):
        """Настроить все хэндлеры"""

        @self.router.message(Command(self.command))
        async def cmd_tiktok(message: Message, state: FSMContext):
            if not self._check_access(message.from_user.id):
                await message.answer("⛔ У вас нет доступа к этой функции")
                return

            await state.set_state(TikTokStates.waiting_for_query)
            await message.answer(
                "🎬 **TikTok Парсер**\n\n"
                "Введите поисковый запрос или хэштег:\n"
                "Например: `смешные коты` или `#funny`\n\n"
                "Для отмены: /cancel",
                parse_mode="Markdown"
            )

        @self.router.message(TikTokStates.waiting_for_query)
        async def get_query(message: Message, state: FSMContext):
            if message.text and message.text.startswith('/'):
                if message.text == '/cancel':
                    await state.clear()
                    await message.answer("❌ Отменено")
                return

            query = message.text.strip() if message.text else ""
            if len(query) < 2:
                await message.answer("⚠️ Запрос слишком короткий. Минимум 2 символа.")
                return

            await state.update_data(query=query)
            await state.set_state(TikTokStates.waiting_for_count)

            builder = InlineKeyboardBuilder()
            counts = [3, 5, 10, 15, 20]
            for count in counts:
                if count <= self.max_videos:
                    builder.button(text=str(count), callback_data=f"tiktok_count:{count}")
            builder.adjust(5)

            await message.answer(
                f"🔍 Запрос: **{query}**\n\n"
                f"Выберите количество видео (макс. {self.max_videos}):",
                reply_markup=builder.as_markup(),
                parse_mode="Markdown"
            )

        @self.router.callback_query(F.data.startswith("tiktok_count:"))
        async def select_count(callback: CallbackQuery, state: FSMContext):
            current_state = await state.get_state()
            if current_state != TikTokStates.waiting_for_count.state:
                await callback.answer("Сессия устарела, начните заново")
                return

            count = int(callback.data.split(":")[1])
            data = await state.get_data()
            query = data.get("query", "")

            await callback.message.edit_text(
                f"🔍 Запрос: **{query}**\n"
                f"📊 Количество: **{count}**\n\n"
                f"⏳ Начинаю поиск...",
                parse_mode="Markdown"
            )

            await state.set_state(TikTokStates.processing)
            await callback.answer()

            await self._process_search(callback.message, query, count, state)

        @self.router.message(TikTokStates.waiting_for_count)
        async def get_count_text(message: Message, state: FSMContext):
            if message.text and message.text.startswith('/'):
                if message.text == '/cancel':
                    await state.clear()
                    await message.answer("❌ Отменено")
                return

            try:
                count = int(message.text.strip()) if message.text else self.default_count
                if count < 1:
                    raise ValueError()
                count = min(count, self.max_videos)
            except ValueError:
                await message.answer(f"⚠️ Введите число от 1 до {self.max_videos}")
                return

            data = await state.get_data()
            query = data.get("query", "")

            status_msg = await message.answer(
                f"🔍 Запрос: **{query}**\n"
                f"📊 Количество: **{count}**\n\n"
                f"⏳ Начинаю поиск...",
                parse_mode="Markdown"
            )

            await state.set_state(TikTokStates.processing)
            await self._process_search(status_msg, query, count, state)

        @self.router.message(F.text.regexp(r'https?://(?:www\.)?(?:tiktok\.com|vm\.tiktok\.com)/'))
        async def handle_tiktok_link(message: Message):
            if not self._check_access(message.from_user.id):
                return

            url_match = re.search(
                r'https?://(?:www\.)?(?:tiktok\.com|vm\.tiktok\.com)/[^\s]+',
                message.text or ""
            )
            if not url_match:
                return

            url = url_match.group()
            status = await message.answer("⏳ Загружаю видео...")

            video_data = await self.parser.get_video_by_url(url)

            if video_data:
                await self.parser.send_video_to_user(message.chat.id, video_data)
                await status.delete()
            else:
                await status.edit_text("❌ Не удалось загрузить видео")

    async def _process_search(self, status_message: Message, query: str, count: int, state: FSMContext):
        """Обработать поисковый запрос"""
        user_id = status_message.chat.id

        if self.on_start_callback:
            await self.on_start_callback(user_id, query, count)

        async def progress_callback(current: int, total: int, video_info: dict):
            try:
                await status_message.edit_text(
                    f"🔍 Запрос: **{query}**\n"
                    f"📊 Загружено: **{current}/{total}**\n\n"
                    f"⏳ Обработка: {video_info.get('author', 'Unknown')}...",
                    parse_mode="Markdown"
                )
            except Exception:
                pass

        try:
            videos = await self.parser.search_and_parse(query, count=count, progress_callback=progress_callback)

            if not videos:
                await status_message.edit_text(
                    f"😔 По запросу **{query}** видео не найдено.\nПопробуйте другой запрос.",
                    parse_mode="Markdown"
                )
                await state.clear()
                return

            await status_message.edit_text(
                f"✅ Найдено **{len(videos)}** видео!\nОтправляю...",
                parse_mode="Markdown"
            )

            sent_count = 0
            for video in videos:
                success = await self.parser.send_video_to_user(user_id, video)
                if success:
                    sent_count += 1
                await asyncio.sleep(0.5)

            await status_message.edit_text(
                f"✅ Готово!\n\n"
                f"🔍 Запрос: **{query}**\n"
                f"📹 Отправлено: **{sent_count}** видео\n\n"
                f"Для нового поиска: /{self.command}",
                parse_mode="Markdown"
            )

            if self.on_complete_callback:
                await self.on_complete_callback(user_id, sent_count)

        except Exception as e:
            print(f"Error in _process_search: {e}")
            await status_message.edit_text("❌ Произошла ошибка при обработке.\nПопробуйте позже.")
        finally:
            await state.clear()

    def _check_access(self, user_id: int) -> bool:
        if self.admin_ids is None:
            return True
        return user_id in self.admin_ids

    def get_router(self) -> Router:
        return self.router

    def register_handlers(self, dp):
        """Зарегистрировать хэндлеры в Dispatcher"""
        dp.include_router(self.router)

    async def close(self):
        await self.parser.close()


# ============================================
# ПРОСТОЙ ХЭНДЛЕР (альтернатива без FSM)
# ============================================

class SimpleTikTokHandler:
    """
    Простой хэндлер без FSM

    Использование:
        handler = SimpleTikTokHandler(bot, storage_chat_id)

        @dp.message(Command("tt"))
        async def tt(message: Message):
            # /tt запрос количество
            await handler.handle_command(message)
    """

    def __init__(self, bot: Bot, storage_chat_id: int):
        self.parser = TikTokParser(bot, storage_chat_id)

    async def handle_command(self, message: Message, default_count: int = 5):
        """Обработать команду: /tt <запрос> [количество]"""
        text = (message.text or "").split(maxsplit=2)

        if len(text) < 2:
            await message.answer("Использование: /tt <запрос> [кол-во]\nПример: /tt смешные коты 5")
            return

        query = text[1]
        count = default_count

        if len(text) > 2:
            try:
                count = int(text[2])
            except ValueError:
                query = f"{text[1]} {text[2]}"

        status = await message.answer(f"🔍 Ищу: {query}...")

        videos = await self.parser.search_and_parse(query, count=count)

        if not videos:
            await status.edit_text("😔 Видео не найдено")
            return

        await status.edit_text(f"📹 Отправляю {len(videos)} видео...")

        for video in videos:
            await self.parser.send_video_to_user(message.chat.id, video)
            await asyncio.sleep(0.5)

        await status.delete()

    async def handle_url(self, message: Message) -> bool:
        """Обработать TikTok ссылку"""
        url_match = re.search(r'https?://(?:www\.)?(?:tiktok\.com|vm\.tiktok\.com)/[^\s]+', message.text or "")
        if not url_match:
            return False

        url = url_match.group()
        status = await message.answer("⏳ Загружаю видео...")

        video_data = await self.parser.get_video_by_url(url)

        if video_data:
            await self.parser.send_video_to_user(message.chat.id, video_data)
            await status.delete()
        else:
            await status.edit_text("❌ Не удалось загрузить видео")

        return True

    async def close(self):
        await self.parser.close()


# ============================================
# ПРИМЕР ИСПОЛЬЗОВАНИЯ
# ============================================

if __name__ == "__main__":
    """
    Пример запуска бота с TikTok парсером
    """
    from aiogram import Dispatcher
    from aiogram.fsm.storage.memory import MemoryStorage

    # НАСТРОЙТЕ ЭТИ ПАРАМЕТРЫ:
    BOT_TOKEN = "YOUR_BOT_TOKEN"
    STORAGE_CHAT_ID = -1001234567890  # ID вашей группы

    async def main():
        bot = Bot(token=BOT_TOKEN)
        dp = Dispatcher(storage=MemoryStorage())

        # Инициализация модуля
        tiktok = TikTokParserModule(
            bot=bot,
            storage_chat_id=STORAGE_CHAT_ID,
            command="tiktok",
            max_videos=20,
            admin_ids=None  # None = доступ всем
        )
        tiktok.register_handlers(dp)

        # Команда /start
        @dp.message(Command("start"))
        async def cmd_start(message: Message):
            await message.answer(
                "👋 Привет!\n\n"
                "/tiktok - Поиск видео\n"
                "Или отправьте ссылку на TikTok видео",
                parse_mode="Markdown"
            )

        print("Bot starting...")
        try:
            await dp.start_polling(bot)
        finally:
            await tiktok.close()
            await bot.session.close()

    asyncio.run(main())
