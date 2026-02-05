"""
TikTok Parser Bot with Video Uniqueization
==========================================

Полноценный Telegram бот для парсинга и уникализации видео с TikTok.

Функционал:
- Парсинг видео по запросу (до 50 штук)
- Скачивание без водяного знака
- Уникализация через FFmpeg (невидимый шум, метаданные, и т.д.)
- Система очередей для обработки множества видео
- Оптимизация нагрузки на сервер

Требования:
    pip install aiogram aiohttp aiofiles
    apt install ffmpeg  # или brew install ffmpeg

Запуск:
    python tiktok_bot.py

Автор: Claude
"""

import asyncio
import os
import re
import uuid
import random
import tempfile
import shutil
import logging
from typing import Optional, Dict, Any, List, Callable
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO
from pathlib import Path
from contextlib import asynccontextmanager

import aiohttp
import aiofiles
from aiogram import Bot, Dispatcher, Router, F
from aiogram.types import (
    Message, CallbackQuery, FSInputFile,
    InlineKeyboardButton, InlineKeyboardMarkup,
    ReplyKeyboardMarkup, KeyboardButton
)
from aiogram.filters import Command, CommandStart
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import InlineKeyboardBuilder, ReplyKeyboardBuilder

# ============================================
# КОНФИГУРАЦИЯ
# ============================================

class Config:
    # Telegram Bot Token (получить у @BotFather)
    BOT_TOKEN: str = "YOUR_BOT_TOKEN_HERE"

    # ID группы для хранения видео (получить через @userinfobot)
    # Создайте приватную группу, добавьте бота админом
    STORAGE_CHAT_ID: int = -1001234567890

    # Максимальное количество видео за один запрос парсинга
    MAX_VIDEOS_PER_REQUEST: int = 50

    # Максимальное количество параллельных FFmpeg процессов
    MAX_CONCURRENT_FFMPEG: int = 2

    # Максимальное количество параллельных скачиваний
    MAX_CONCURRENT_DOWNLOADS: int = 3

    # Максимальный размер очереди на пользователя
    MAX_QUEUE_PER_USER: int = 100

    # Временная директория для обработки видео
    TEMP_DIR: str = "/tmp/tiktok_bot"

    # Таймаут HTTP запросов (секунды)
    REQUEST_TIMEOUT: int = 60

    # Максимальная длительность видео (секунды)
    MAX_VIDEO_DURATION: int = 300

    # Список ID админов (None = доступ всем)
    ADMIN_IDS: Optional[List[int]] = None

    # Логирование
    LOG_LEVEL: str = "INFO"


# Настройка логирования
logging.basicConfig(
    level=getattr(logging, Config.LOG_LEVEL),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


# ============================================
# FSM STATES
# ============================================

class BotStates(StatesGroup):
    """Состояния бота"""
    main_menu = State()
    waiting_query = State()
    waiting_count = State()
    processing = State()
    waiting_video_for_unique = State()


# ============================================
# DATA CLASSES
# ============================================

@dataclass
class VideoTask:
    """Задача на обработку видео"""
    task_id: str
    user_id: int
    chat_id: int
    video_url: Optional[str] = None
    file_id: Optional[str] = None
    file_path: Optional[str] = None
    author: str = ""
    title: str = ""
    status: str = "pending"  # pending, downloading, processing, sending, done, error
    error_message: str = ""
    created_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        if not self.task_id:
            self.task_id = str(uuid.uuid4())[:8]


@dataclass
class UserQueue:
    """Очередь пользователя"""
    user_id: int
    tasks: List[VideoTask] = field(default_factory=list)
    is_processing: bool = False


# ============================================
# VIDEO UNIQUEIZER (FFmpeg)
# ============================================

class VideoUniqueizer:
    """
    Уникализация видео через FFmpeg

    Применяет множество невидимых изменений:
    - Невидимый шум
    - Микро-изменение скорости
    - Сдвиг цветов
    - Изменение метаданных
    - Случайный битрейт
    - Невидимый водяной знак
    """

    def __init__(self, temp_dir: str = "/tmp/tiktok_bot"):
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self._semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_FFMPEG)

    async def uniqueize(self, input_path: str, output_path: Optional[str] = None) -> Optional[str]:
        """
        Уникализировать видео

        Args:
            input_path: Путь к исходному видео
            output_path: Путь для сохранения (если None - генерируется)

        Returns:
            Путь к уникализированному видео или None при ошибке
        """
        async with self._semaphore:
            return await self._process_video(input_path, output_path)

    async def _process_video(self, input_path: str, output_path: Optional[str] = None) -> Optional[str]:
        """Внутренняя обработка видео"""
        if not output_path:
            output_path = str(self.temp_dir / f"unique_{uuid.uuid4().hex[:8]}.mp4")

        # Генерируем случайные параметры для уникализации
        params = self._generate_random_params()

        # Формируем FFmpeg команду
        cmd = self._build_ffmpeg_command(input_path, output_path, params)

        logger.debug(f"FFmpeg command: {' '.join(cmd)}")

        try:
            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE
            )

            stdout, stderr = await asyncio.wait_for(
                process.communicate(),
                timeout=300  # 5 минут максимум
            )

            if process.returncode != 0:
                logger.error(f"FFmpeg error: {stderr.decode()}")
                return None

            # Проверяем что файл создан
            if os.path.exists(output_path) and os.path.getsize(output_path) > 0:
                return output_path

            return None

        except asyncio.TimeoutError:
            logger.error("FFmpeg timeout")
            return None
        except Exception as e:
            logger.error(f"FFmpeg exception: {e}")
            return None

    def _generate_random_params(self) -> Dict[str, Any]:
        """Генерация случайных параметров уникализации"""
        return {
            # Скорость видео (почти незаметное изменение)
            "speed": random.uniform(0.98, 1.02),

            # Шум (очень слабый, невидимый)
            "noise_strength": random.uniform(0.5, 2.0),

            # Сдвиг яркости
            "brightness": random.uniform(-0.02, 0.02),

            # Сдвиг контраста
            "contrast": random.uniform(0.98, 1.02),

            # Сдвиг насыщенности
            "saturation": random.uniform(0.98, 1.02),

            # Гамма
            "gamma": random.uniform(0.98, 1.02),

            # Сдвиг оттенка (в градусах)
            "hue_shift": random.uniform(-2, 2),

            # Битрейт видео
            "video_bitrate": random.choice(["2M", "2.5M", "3M", "3.5M", "4M"]),

            # Битрейт аудио
            "audio_bitrate": random.choice(["128k", "160k", "192k"]),

            # Изменение громкости
            "volume": random.uniform(0.95, 1.05),

            # Случайные метаданные
            "metadata_title": f"video_{uuid.uuid4().hex[:6]}",
            "metadata_comment": str(uuid.uuid4()),

            # Небольшой crop (1-2 пикселя с каждой стороны)
            "crop_pixels": random.randint(1, 3),

            # Поворот на микро-угол
            "rotation": random.uniform(-0.5, 0.5),
        }

    def _build_ffmpeg_command(self, input_path: str, output_path: str, params: Dict[str, Any]) -> List[str]:
        """Построение FFmpeg команды"""

        # Видеофильтры
        video_filters = [
            # Небольшой crop для изменения разрешения
            f"crop=iw-{params['crop_pixels']*2}:ih-{params['crop_pixels']*2}:{params['crop_pixels']}:{params['crop_pixels']}",

            # Масштабирование обратно (pad вместо scale для сохранения качества)
            "scale=trunc(iw/2)*2:trunc(ih/2)*2",

            # Коррекция цвета
            f"eq=brightness={params['brightness']}:contrast={params['contrast']}:saturation={params['saturation']}:gamma={params['gamma']}",

            # Сдвиг оттенка
            f"hue=h={params['hue_shift']}",

            # Добавление шума (очень слабого)
            f"noise=alls={params['noise_strength']}:allf=t",

            # Изменение скорости
            f"setpts={1/params['speed']}*PTS",

            # Микро-поворот
            f"rotate={params['rotation']}*PI/180",
        ]

        # Аудиофильтры
        audio_filters = [
            # Изменение скорости аудио (синхронно с видео)
            f"atempo={params['speed']}",

            # Изменение громкости
            f"volume={params['volume']}",

            # Небольшой сдвиг высоты тона (почти незаметный)
            f"asetrate=44100*{random.uniform(0.99, 1.01)},aresample=44100",
        ]

        cmd = [
            "ffmpeg",
            "-y",  # Перезаписывать файлы
            "-i", input_path,

            # Видеофильтры
            "-vf", ",".join(video_filters),

            # Аудиофильтры
            "-af", ",".join(audio_filters),

            # Видеокодек
            "-c:v", "libx264",
            "-preset", "fast",  # Быстрее, меньше нагрузка
            "-crf", "23",  # Качество
            "-b:v", params["video_bitrate"],

            # Аудиокодек
            "-c:a", "aac",
            "-b:a", params["audio_bitrate"],

            # Метаданные (очистка старых + новые случайные)
            "-map_metadata", "-1",
            "-metadata", f"title={params['metadata_title']}",
            "-metadata", f"comment={params['metadata_comment']}",
            "-metadata", f"creation_time={datetime.now().isoformat()}",

            # Оптимизация для веба
            "-movflags", "+faststart",

            # Ограничение потоков для снижения нагрузки
            "-threads", "2",

            output_path
        ]

        return cmd

    async def cleanup_file(self, file_path: str):
        """Удалить временный файл"""
        try:
            if os.path.exists(file_path):
                os.remove(file_path)
        except Exception as e:
            logger.warning(f"Failed to cleanup {file_path}: {e}")


# ============================================
# TIKTOK DOWNLOADER
# ============================================

class TikTokDownloader:
    """Скачивание видео с TikTok без водяного знака"""

    TIKWM_API = "https://www.tikwm.com/api/"
    TIKWM_FEED_API = "https://www.tikwm.com/api/feed/search"

    def __init__(self, temp_dir: str = "/tmp/tiktok_bot"):
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.timeout = aiohttp.ClientTimeout(total=Config.REQUEST_TIMEOUT)
        self._session: Optional[aiohttp.ClientSession] = None
        self._download_semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_DOWNLOADS)

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

    async def search_videos(self, query: str, count: int = 10) -> List[Dict[str, Any]]:
        """Поиск видео по запросу"""
        session = await self._get_session()
        query = query.lstrip('#')
        videos = []
        cursor = 0
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
                        duration = v.get("duration", 0)
                        if duration > Config.MAX_VIDEO_DURATION:
                            continue

                        videos.append({
                            "video_url": v.get("play", ""),
                            "author": v.get("author", {}).get("nickname", "Unknown"),
                            "author_id": v.get("author", {}).get("unique_id", ""),
                            "title": v.get("title", ""),
                            "play_count": v.get("play_count", 0),
                            "like_count": v.get("digg_count", 0),
                            "duration": duration,
                            "video_id": v.get("video_id", ""),
                        })

                    remaining -= len(batch_videos)
                    cursor = data.get("data", {}).get("cursor", cursor + batch_count)

                    if not data.get("data", {}).get("hasMore", False):
                        break

                    await asyncio.sleep(0.3)

            except Exception as e:
                logger.error(f"Error searching videos: {e}")
                break

        return videos[:count]

    async def download_video(self, video_url: str) -> Optional[str]:
        """Скачать видео во временный файл"""
        async with self._download_semaphore:
            session = await self._get_session()
            output_path = str(self.temp_dir / f"download_{uuid.uuid4().hex[:8]}.mp4")

            try:
                async with session.get(video_url) as response:
                    if response.status != 200:
                        return None

                    async with aiofiles.open(output_path, 'wb') as f:
                        async for chunk in response.content.iter_chunked(8192):
                            await f.write(chunk)

                    return output_path

            except Exception as e:
                logger.error(f"Error downloading video: {e}")
                if os.path.exists(output_path):
                    os.remove(output_path)
                return None

    async def get_video_info(self, url: str) -> Optional[Dict[str, Any]]:
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
                return {
                    "video_url": v.get("play", ""),
                    "author": v.get("author", {}).get("nickname", "Unknown"),
                    "author_id": v.get("author", {}).get("unique_id", ""),
                    "title": v.get("title", ""),
                    "duration": v.get("duration", 0),
                }
        except Exception as e:
            logger.error(f"Error getting video info: {e}")
            return None


# ============================================
# QUEUE MANAGER
# ============================================

class QueueManager:
    """Менеджер очередей для обработки видео"""

    def __init__(self, bot: Bot, downloader: TikTokDownloader, uniqueizer: VideoUniqueizer):
        self.bot = bot
        self.downloader = downloader
        self.uniqueizer = uniqueizer
        self._user_queues: Dict[int, UserQueue] = {}
        self._processing_tasks: Dict[int, asyncio.Task] = {}

    def get_user_queue(self, user_id: int) -> UserQueue:
        """Получить очередь пользователя"""
        if user_id not in self._user_queues:
            self._user_queues[user_id] = UserQueue(user_id=user_id)
        return self._user_queues[user_id]

    def add_task(self, task: VideoTask) -> bool:
        """Добавить задачу в очередь"""
        queue = self.get_user_queue(task.user_id)

        if len(queue.tasks) >= Config.MAX_QUEUE_PER_USER:
            return False

        queue.tasks.append(task)
        logger.info(f"Task {task.task_id} added to queue for user {task.user_id}")

        # Запускаем обработку если не запущена
        if task.user_id not in self._processing_tasks or self._processing_tasks[task.user_id].done():
            self._processing_tasks[task.user_id] = asyncio.create_task(
                self._process_user_queue(task.user_id)
            )

        return True

    def get_queue_status(self, user_id: int) -> Dict[str, int]:
        """Получить статус очереди пользователя"""
        queue = self.get_user_queue(user_id)
        status_counts = {
            "pending": 0,
            "downloading": 0,
            "processing": 0,
            "sending": 0,
            "done": 0,
            "error": 0
        }

        for task in queue.tasks:
            status_counts[task.status] = status_counts.get(task.status, 0) + 1

        return status_counts

    def clear_completed(self, user_id: int):
        """Очистить завершенные задачи"""
        queue = self.get_user_queue(user_id)
        queue.tasks = [t for t in queue.tasks if t.status not in ("done", "error")]

    async def _process_user_queue(self, user_id: int):
        """Обработать очередь пользователя"""
        queue = self.get_user_queue(user_id)
        queue.is_processing = True

        try:
            while True:
                # Находим следующую задачу для обработки
                pending_task = None
                for task in queue.tasks:
                    if task.status == "pending":
                        pending_task = task
                        break

                if not pending_task:
                    break

                await self._process_task(pending_task)

                # Небольшая задержка между задачами
                await asyncio.sleep(0.5)

        except Exception as e:
            logger.error(f"Error processing queue for user {user_id}: {e}")
        finally:
            queue.is_processing = False

    async def _process_task(self, task: VideoTask):
        """Обработать одну задачу"""
        downloaded_path = None
        unique_path = None

        try:
            # Шаг 1: Скачивание
            task.status = "downloading"

            if task.video_url:
                downloaded_path = await self.downloader.download_video(task.video_url)
            elif task.file_id:
                # Скачиваем файл из Telegram
                downloaded_path = str(self.uniqueizer.temp_dir / f"tg_{uuid.uuid4().hex[:8]}.mp4")
                file = await self.bot.get_file(task.file_id)
                await self.bot.download_file(file.file_path, downloaded_path)

            if not downloaded_path:
                task.status = "error"
                task.error_message = "Не удалось скачать видео"
                await self._send_error(task)
                return

            # Шаг 2: Уникализация
            task.status = "processing"
            unique_path = await self.uniqueizer.uniqueize(downloaded_path)

            if not unique_path:
                task.status = "error"
                task.error_message = "Не удалось обработать видео"
                await self._send_error(task)
                return

            # Шаг 3: Отправка
            task.status = "sending"

            caption_parts = []
            if task.author:
                caption_parts.append(f"🎬 {task.author}")
            if task.title:
                caption_parts.append(task.title[:200])
            caption_parts.append("✅ Уникализировано")

            caption = "\n".join(caption_parts)

            video_file = FSInputFile(unique_path)
            await self.bot.send_video(
                chat_id=task.chat_id,
                video=video_file,
                caption=caption,
                supports_streaming=True
            )

            task.status = "done"
            logger.info(f"Task {task.task_id} completed successfully")

        except Exception as e:
            logger.error(f"Error processing task {task.task_id}: {e}")
            task.status = "error"
            task.error_message = str(e)
            await self._send_error(task)

        finally:
            # Очистка временных файлов
            if downloaded_path:
                await self.uniqueizer.cleanup_file(downloaded_path)
            if unique_path:
                await self.uniqueizer.cleanup_file(unique_path)

    async def _send_error(self, task: VideoTask):
        """Отправить сообщение об ошибке"""
        try:
            await self.bot.send_message(
                chat_id=task.chat_id,
                text=f"❌ Ошибка обработки видео\n{task.error_message}"
            )
        except Exception:
            pass


# ============================================
# KEYBOARDS
# ============================================

def get_main_keyboard() -> ReplyKeyboardMarkup:
    """Главное меню"""
    builder = ReplyKeyboardBuilder()
    builder.row(
        KeyboardButton(text="🎬 Парсер TikTok"),
        KeyboardButton(text="🔄 Уникализация")
    )
    builder.row(
        KeyboardButton(text="📊 Статус очереди"),
        KeyboardButton(text="❓ Помощь")
    )
    return builder.as_markup(resize_keyboard=True)


def get_count_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура выбора количества"""
    builder = InlineKeyboardBuilder()

    counts = [5, 10, 15, 20, 30, 50]
    for count in counts:
        builder.button(text=str(count), callback_data=f"count:{count}")

    builder.adjust(3, 3)
    builder.row(InlineKeyboardButton(text="❌ Отмена", callback_data="cancel"))

    return builder.as_markup()


def get_cancel_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура отмены"""
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="❌ Отмена", callback_data="cancel")]
    ])


# ============================================
# BOT HANDLERS
# ============================================

class TikTokBot:
    """Основной класс бота"""

    def __init__(self, token: str, storage_chat_id: int):
        self.bot = Bot(token=token)
        self.dp = Dispatcher(storage=MemoryStorage())
        self.router = Router()
        self.storage_chat_id = storage_chat_id

        # Компоненты
        self.downloader = TikTokDownloader(Config.TEMP_DIR)
        self.uniqueizer = VideoUniqueizer(Config.TEMP_DIR)
        self.queue_manager = QueueManager(self.bot, self.downloader, self.uniqueizer)

        # Настройка хэндлеров
        self._setup_handlers()
        self.dp.include_router(self.router)

    def _setup_handlers(self):
        """Настройка всех хэндлеров"""

        # /start
        @self.router.message(CommandStart())
        async def cmd_start(message: Message, state: FSMContext):
            await state.clear()
            await message.answer(
                "👋 **Привет!**\n\n"
                "Я бот для парсинга и уникализации видео с TikTok.\n\n"
                "**Возможности:**\n"
                "🎬 Парсер - поиск и скачивание видео по запросу\n"
                "🔄 Уникализация - обработка видео для соцсетей\n\n"
                "Выберите действие:",
                reply_markup=get_main_keyboard(),
                parse_mode="Markdown"
            )

        # Главное меню - Парсер
        @self.router.message(F.text == "🎬 Парсер TikTok")
        async def menu_parser(message: Message, state: FSMContext):
            await state.set_state(BotStates.waiting_query)
            await message.answer(
                "🔍 **Парсер TikTok**\n\n"
                "Введите поисковый запрос:\n"
                "Например: `смешные коты`, `#funny`, `танцы`",
                reply_markup=get_cancel_keyboard(),
                parse_mode="Markdown"
            )

        # Главное меню - Уникализация
        @self.router.message(F.text == "🔄 Уникализация")
        async def menu_uniqueize(message: Message, state: FSMContext):
            await state.set_state(BotStates.waiting_video_for_unique)
            await message.answer(
                "🔄 **Уникализация видео**\n\n"
                "Отправьте видео или ссылку на TikTok.\n"
                "Можно отправить сразу несколько видео!\n\n"
                "Видео будет обработано и возвращено с изменениями,\n"
                "невидимыми для глаза, но уникальными для соцсетей.",
                reply_markup=get_cancel_keyboard(),
                parse_mode="Markdown"
            )

        # Главное меню - Статус
        @self.router.message(F.text == "📊 Статус очереди")
        async def menu_status(message: Message):
            status = self.queue_manager.get_queue_status(message.from_user.id)

            total = sum(status.values())
            if total == 0:
                await message.answer("📊 Очередь пуста")
                return

            text = (
                "📊 **Статус очереди:**\n\n"
                f"⏳ Ожидают: {status['pending']}\n"
                f"📥 Скачивается: {status['downloading']}\n"
                f"🔄 Обрабатывается: {status['processing']}\n"
                f"📤 Отправляется: {status['sending']}\n"
                f"✅ Готово: {status['done']}\n"
                f"❌ Ошибок: {status['error']}\n\n"
                f"📦 Всего: {total}"
            )

            await message.answer(text, parse_mode="Markdown")

            # Очищаем завершенные
            self.queue_manager.clear_completed(message.from_user.id)

        # Главное меню - Помощь
        @self.router.message(F.text == "❓ Помощь")
        async def menu_help(message: Message):
            await message.answer(
                "❓ **Помощь**\n\n"
                "**🎬 Парсер TikTok**\n"
                "Введите запрос → выберите количество → получите уникальные видео!\n\n"
                "**🔄 Уникализация**\n"
                "Отправьте свои видео или ссылки TikTok.\n"
                "Бот обработает их через FFmpeg:\n"
                "• Невидимый шум\n"
                "• Изменение метаданных\n"
                "• Микро-коррекция цвета\n"
                "• Изменение битрейта\n\n"
                "**📊 Очередь**\n"
                "Видео обрабатываются по очереди.\n"
                f"Максимум {Config.MAX_QUEUE_PER_USER} видео в очереди.\n\n"
                "**Команды:**\n"
                "/start - Главное меню\n"
                "/cancel - Отмена действия",
                parse_mode="Markdown"
            )

        # Отмена
        @self.router.message(Command("cancel"))
        @self.router.callback_query(F.data == "cancel")
        async def cancel_action(event: Message | CallbackQuery, state: FSMContext):
            await state.clear()

            if isinstance(event, CallbackQuery):
                await event.message.edit_text("❌ Отменено")
                await event.answer()
            else:
                await event.answer("❌ Отменено", reply_markup=get_main_keyboard())

        # Получение запроса для парсера
        @self.router.message(BotStates.waiting_query)
        async def get_search_query(message: Message, state: FSMContext):
            if not message.text:
                return

            query = message.text.strip()
            if len(query) < 2:
                await message.answer("⚠️ Запрос слишком короткий")
                return

            await state.update_data(query=query)
            await state.set_state(BotStates.waiting_count)

            await message.answer(
                f"🔍 Запрос: **{query}**\n\n"
                f"Выберите количество видео (до {Config.MAX_VIDEOS_PER_REQUEST}):",
                reply_markup=get_count_keyboard(),
                parse_mode="Markdown"
            )

        # Выбор количества
        @self.router.callback_query(F.data.startswith("count:"))
        async def select_count(callback: CallbackQuery, state: FSMContext):
            current_state = await state.get_state()
            if current_state != BotStates.waiting_count.state:
                await callback.answer("Сессия устарела")
                return

            count = int(callback.data.split(":")[1])
            data = await state.get_data()
            query = data.get("query", "")

            await callback.message.edit_text(
                f"🔍 Запрос: **{query}**\n"
                f"📊 Количество: **{count}**\n\n"
                f"⏳ Поиск видео...",
                parse_mode="Markdown"
            )

            await callback.answer()
            await state.set_state(BotStates.processing)

            # Запускаем парсинг
            await self._start_parsing(callback.message, query, count, callback.from_user.id, state)

        # Ввод количества текстом
        @self.router.message(BotStates.waiting_count)
        async def get_count_text(message: Message, state: FSMContext):
            try:
                count = int(message.text.strip())
                if count < 1 or count > Config.MAX_VIDEOS_PER_REQUEST:
                    raise ValueError()
            except (ValueError, AttributeError):
                await message.answer(f"⚠️ Введите число от 1 до {Config.MAX_VIDEOS_PER_REQUEST}")
                return

            data = await state.get_data()
            query = data.get("query", "")

            status_msg = await message.answer(
                f"🔍 Запрос: **{query}**\n"
                f"📊 Количество: **{count}**\n\n"
                f"⏳ Поиск видео...",
                parse_mode="Markdown"
            )

            await state.set_state(BotStates.processing)
            await self._start_parsing(status_msg, query, count, message.from_user.id, state)

        # Обработка видео для уникализации
        @self.router.message(BotStates.waiting_video_for_unique, F.video)
        async def handle_video_for_unique(message: Message):
            task = VideoTask(
                task_id="",
                user_id=message.from_user.id,
                chat_id=message.chat.id,
                file_id=message.video.file_id,
                author="",
                title="Ваше видео"
            )

            if self.queue_manager.add_task(task):
                status = self.queue_manager.get_queue_status(message.from_user.id)
                await message.answer(
                    f"✅ Видео добавлено в очередь\n"
                    f"📦 В очереди: {sum(status.values())} видео\n\n"
                    f"Можете отправить еще видео или вернуться в меню /start"
                )
            else:
                await message.answer("❌ Очередь переполнена. Подождите завершения обработки.")

        # Обработка ссылки TikTok для уникализации
        @self.router.message(BotStates.waiting_video_for_unique, F.text.regexp(r'tiktok\.com|vm\.tiktok\.com'))
        async def handle_tiktok_link_for_unique(message: Message):
            url_match = re.search(
                r'https?://(?:www\.)?(?:tiktok\.com|vm\.tiktok\.com)/[^\s]+',
                message.text or ""
            )

            if not url_match:
                await message.answer("⚠️ Не удалось распознать ссылку")
                return

            status_msg = await message.answer("🔍 Получаю информацию о видео...")

            info = await self.downloader.get_video_info(url_match.group())

            if not info:
                await status_msg.edit_text("❌ Не удалось получить видео по ссылке")
                return

            task = VideoTask(
                task_id="",
                user_id=message.from_user.id,
                chat_id=message.chat.id,
                video_url=info["video_url"],
                author=info.get("author", ""),
                title=info.get("title", "")
            )

            if self.queue_manager.add_task(task):
                status = self.queue_manager.get_queue_status(message.from_user.id)
                await status_msg.edit_text(
                    f"✅ Видео от @{info.get('author_id', 'unknown')} добавлено в очередь\n"
                    f"📦 В очереди: {sum(status.values())} видео\n\n"
                    f"Можете отправить еще видео или вернуться в меню /start"
                )
            else:
                await status_msg.edit_text("❌ Очередь переполнена")

        # Обработка TikTok ссылок в любом состоянии (кроме ожидания уникализации)
        @self.router.message(F.text.regexp(r'https?://(?:www\.)?(?:tiktok\.com|vm\.tiktok\.com)/'))
        async def handle_tiktok_link_global(message: Message, state: FSMContext):
            current_state = await state.get_state()

            # Если мы в режиме уникализации, обработано выше
            if current_state == BotStates.waiting_video_for_unique.state:
                return

            # Иначе предлагаем выбор
            url_match = re.search(
                r'https?://(?:www\.)?(?:tiktok\.com|vm\.tiktok\.com)/[^\s]+',
                message.text or ""
            )

            if not url_match:
                return

            builder = InlineKeyboardBuilder()
            builder.button(text="🔄 Уникализировать", callback_data=f"unique_link:{url_match.group()[:60]}")
            builder.button(text="❌ Отмена", callback_data="cancel")
            builder.adjust(1)

            await state.update_data(tiktok_url=url_match.group())

            await message.answer(
                "🔗 Обнаружена ссылка на TikTok\n\n"
                "Что сделать с этим видео?",
                reply_markup=builder.as_markup()
            )

        # Уникализация по ссылке из глобального хэндлера
        @self.router.callback_query(F.data.startswith("unique_link:"))
        async def unique_link_callback(callback: CallbackQuery, state: FSMContext):
            data = await state.get_data()
            url = data.get("tiktok_url", "")

            if not url:
                await callback.answer("Ссылка устарела")
                return

            await callback.message.edit_text("🔍 Получаю информацию о видео...")
            await callback.answer()

            info = await self.downloader.get_video_info(url)

            if not info:
                await callback.message.edit_text("❌ Не удалось получить видео")
                return

            task = VideoTask(
                task_id="",
                user_id=callback.from_user.id,
                chat_id=callback.message.chat.id,
                video_url=info["video_url"],
                author=info.get("author", ""),
                title=info.get("title", "")
            )

            if self.queue_manager.add_task(task):
                await callback.message.edit_text(
                    f"✅ Видео добавлено в очередь на уникализацию\n"
                    f"👤 Автор: {info.get('author', 'Unknown')}"
                )
            else:
                await callback.message.edit_text("❌ Очередь переполнена")

            await state.clear()

    async def _start_parsing(
        self,
        status_message: Message,
        query: str,
        count: int,
        user_id: int,
        state: FSMContext
    ):
        """Запустить парсинг видео"""
        try:
            # Поиск видео
            videos = await self.downloader.search_videos(query, count)

            if not videos:
                await status_message.edit_text(
                    f"😔 По запросу **{query}** видео не найдено.\n"
                    "Попробуйте другой запрос.",
                    parse_mode="Markdown"
                )
                await state.clear()
                return

            await status_message.edit_text(
                f"✅ Найдено **{len(videos)}** видео!\n\n"
                f"Добавляю в очередь на обработку...",
                parse_mode="Markdown"
            )

            # Добавляем задачи в очередь
            added = 0
            for video in videos:
                task = VideoTask(
                    task_id="",
                    user_id=user_id,
                    chat_id=status_message.chat.id,
                    video_url=video["video_url"],
                    author=video.get("author", ""),
                    title=video.get("title", "")
                )

                if self.queue_manager.add_task(task):
                    added += 1

            await status_message.edit_text(
                f"✅ **Готово!**\n\n"
                f"🔍 Запрос: {query}\n"
                f"📦 Добавлено в очередь: {added} видео\n\n"
                f"Видео будут обработаны и отправлены по мере готовности.\n"
                f"Проверить статус: 📊 Статус очереди",
                parse_mode="Markdown"
            )

        except Exception as e:
            logger.error(f"Error in parsing: {e}")
            await status_message.edit_text("❌ Произошла ошибка при поиске видео")

        finally:
            await state.clear()

    async def start(self):
        """Запустить бота"""
        logger.info("Starting bot...")

        # Создаем временную директорию
        Path(Config.TEMP_DIR).mkdir(parents=True, exist_ok=True)

        try:
            await self.dp.start_polling(self.bot)
        finally:
            await self.cleanup()

    async def cleanup(self):
        """Очистка ресурсов"""
        await self.downloader.close()

        # Очищаем временные файлы
        temp_dir = Path(Config.TEMP_DIR)
        if temp_dir.exists():
            shutil.rmtree(temp_dir, ignore_errors=True)

        await self.bot.session.close()
        logger.info("Bot stopped")


# ============================================
# ENTRY POINT
# ============================================

def main():
    """Точка входа"""

    # Проверка токена
    if Config.BOT_TOKEN == "YOUR_BOT_TOKEN_HERE":
        print("=" * 50)
        print("ОШИБКА: Не указан BOT_TOKEN!")
        print()
        print("1. Откройте файл и найдите класс Config")
        print("2. Замените YOUR_BOT_TOKEN_HERE на токен от @BotFather")
        print("3. Укажите STORAGE_CHAT_ID (ID группы для хранения)")
        print("=" * 50)
        return

    # Проверка FFmpeg
    if shutil.which("ffmpeg") is None:
        print("=" * 50)
        print("ОШИБКА: FFmpeg не установлен!")
        print()
        print("Установите FFmpeg:")
        print("  Ubuntu/Debian: sudo apt install ffmpeg")
        print("  macOS: brew install ffmpeg")
        print("  Windows: скачайте с ffmpeg.org")
        print("=" * 50)
        return

    # Запуск бота
    bot = TikTokBot(
        token=Config.BOT_TOKEN,
        storage_chat_id=Config.STORAGE_CHAT_ID
    )

    asyncio.run(bot.start())


if __name__ == "__main__":
    main()
