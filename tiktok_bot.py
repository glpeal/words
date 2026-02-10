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
from typing import Optional, Dict, Any, List, Callable, Tuple
from dataclasses import dataclass, field
from datetime import datetime, timedelta
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
from aiogram.exceptions import TelegramRetryAfter

# ============================================
# КОНФИГУРАЦИЯ
# ============================================

# Папка где лежит скрипт
SCRIPT_DIR = Path(__file__).parent.absolute()


def find_ffmpeg() -> str:
    """
    Найти путь к FFmpeg.
    Сначала ищет в папке со скриптом, потом в системном PATH.
    """
    # Возможные имена файла
    ffmpeg_names = ["ffmpeg.exe", "ffmpeg"]

    # 1. Ищем в папке со скриптом
    for name in ffmpeg_names:
        local_path = SCRIPT_DIR / name
        if local_path.exists():
            return str(local_path)

    # 2. Ищем в подпапке ffmpeg (если распаковали архив)
    for subdir in ["ffmpeg", "ffmpeg/bin", "bin"]:
        for name in ffmpeg_names:
            local_path = SCRIPT_DIR / subdir / name
            if local_path.exists():
                return str(local_path)

    # 3. Ищем в системном PATH
    system_ffmpeg = shutil.which("ffmpeg")
    if system_ffmpeg:
        return system_ffmpeg

    return ""


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

    # Временная директория для обработки видео (автоматически для Windows/Linux)
    TEMP_DIR: str = str(SCRIPT_DIR / "temp_videos")

    # Таймаут HTTP запросов (секунды)
    REQUEST_TIMEOUT: int = 60

    # Максимальная длительность видео (секунды)
    MAX_VIDEO_DURATION: int = 300

    # Список ID админов (None = доступ всем)
    ADMIN_IDS: Optional[List[int]] = None

    # Логирование
    LOG_LEVEL: str = "INFO"

    # Путь к FFmpeg (автоматически определяется)
    FFMPEG_PATH: str = ""

    # Время хранения видео в storage группе (секунды) - 10 минут
    STORAGE_VIDEO_TTL: int = 600

    # Файл для сохранения кэша видео
    CACHE_FILE: str = str(SCRIPT_DIR / "video_cache.json")

    # Папка с overlay изображениями для уникализации
    OVERLAYS_DIR: str = str(SCRIPT_DIR / "overlays")

    # Использовать размытый фон по умолчанию
    USE_BLUR_BACKGROUND: bool = False

    # Задержка между отправкой видео в Telegram (секунды)
    SEND_DELAY: float = 1.5

    # Максимальное количество попыток отправки при flood control
    MAX_RETRY_ATTEMPTS: int = 3


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
    settings = State()


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
    video_id: str = ""  # ID видео TikTok для кэширования
    author: str = ""
    author_id: str = ""
    title: str = ""
    use_blur_background: bool = False  # Использовать размытый фон
    use_snow_effect: bool = False  # Добавить плавающую точку (снег)
    skip_uniqueization: bool = False  # Пропустить уникализацию (только скачать)
    do_uniqueize: bool = False  # Выполнить только уникализацию (из очереди)
    status: str = "pending"  # pending, downloading, processing, sending, done, error
    error_message: str = ""
    created_at: datetime = field(default_factory=datetime.now)

    def __post_init__(self):
        if not self.task_id:
            self.task_id = str(uuid.uuid4())[:8]


# ============================================
# USER SETTINGS
# ============================================

class UserSettings:
    """Хранение настроек пользователей"""

    def __init__(self, settings_file: str = None):
        self.settings_file = Path(settings_file) if settings_file else Path(SCRIPT_DIR / "user_settings.json")
        self._settings: Dict[int, Dict[str, Any]] = {}
        self._load()

    def _load(self):
        """Загрузить настройки"""
        try:
            if self.settings_file.exists():
                import json
                with open(self.settings_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                self._settings = {int(k): v for k, v in data.items()}
                logger.info(f"Loaded settings for {len(self._settings)} users")
        except Exception as e:
            logger.warning(f"Failed to load user settings: {e}")

    def _save(self):
        """Сохранить настройки"""
        try:
            import json
            with open(self.settings_file, 'w', encoding='utf-8') as f:
                json.dump(self._settings, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save user settings: {e}")

    def get(self, user_id: int, key: str, default: Any = None) -> Any:
        """Получить настройку пользователя"""
        return self._settings.get(user_id, {}).get(key, default)

    def set(self, user_id: int, key: str, value: Any):
        """Установить настройку пользователя"""
        if user_id not in self._settings:
            self._settings[user_id] = {}
        self._settings[user_id][key] = value
        self._save()

    def get_blur_background(self, user_id: int) -> bool:
        """Получить настройку размытого фона"""
        return self.get(user_id, "blur_background", Config.USE_BLUR_BACKGROUND)

    def set_blur_background(self, user_id: int, value: bool):
        """Установить настройку размытого фона"""
        self.set(user_id, "blur_background", value)

    def get_unique_queue(self, user_id: int) -> List[Dict[str, Any]]:
        """Получить очередь на уникализацию"""
        return self.get(user_id, "unique_queue", [])

    def add_to_unique_queue(self, user_id: int, video_data: Dict[str, Any]) -> bool:
        """Добавить видео в очередь на уникализацию"""
        queue = self.get_unique_queue(user_id)
        # Проверяем, что видео еще не в очереди
        for v in queue:
            if v.get("file_id") == video_data.get("file_id"):
                return False
        queue.append(video_data)
        self.set(user_id, "unique_queue", queue)
        return True

    def remove_from_unique_queue(self, user_id: int, file_id: str) -> bool:
        """Удалить видео из очереди на уникализацию"""
        queue = self.get_unique_queue(user_id)
        new_queue = [v for v in queue if v.get("file_id") != file_id]
        if len(new_queue) != len(queue):
            self.set(user_id, "unique_queue", new_queue)
            return True
        return False

    def clear_unique_queue(self, user_id: int):
        """Очистить очередь на уникализацию"""
        self.set(user_id, "unique_queue", [])

    def get_unique_queue_count(self, user_id: int) -> int:
        """Получить количество видео в очереди на уникализацию"""
        return len(self.get_unique_queue(user_id))

    def get_year_filter(self, user_id: int) -> Dict[str, Any]:
        """
        Получить настройки фильтра по году.
        Возвращает: {"mode": "off"|"only"|"exclude", "year": int|None}
        """
        return self.get(user_id, "year_filter", {"mode": "off", "year": None})

    def set_year_filter(self, user_id: int, mode: str, year: Optional[int] = None):
        """
        Установить фильтр по году.
        mode: "off" - выключен, "only" - только этот год, "exclude" - исключить этот год
        """
        self.set(user_id, "year_filter", {"mode": mode, "year": year})

    def clear_year_filter(self, user_id: int):
        """Сбросить фильтр по году"""
        self.set(user_id, "year_filter", {"mode": "off", "year": None})

    def get_snow_effect(self, user_id: int) -> bool:
        """Получить настройку эффекта снега (плавающая точка)"""
        return self.get(user_id, "snow_effect", False)

    def set_snow_effect(self, user_id: int, value: bool):
        """Установить настройку эффекта снега"""
        self.set(user_id, "snow_effect", value)


@dataclass
class UserQueue:
    """Очередь пользователя"""
    user_id: int
    tasks: List[VideoTask] = field(default_factory=list)
    is_processing: bool = False


@dataclass
class CachedVideo:
    """Информация о кэшированном видео"""
    video_id: str  # ID видео TikTok
    video_url: str  # URL для скачивания
    author_id: str  # ID автора
    title: str
    file_id: str  # Telegram file_id для быстрой пересылки
    cached_at: str  # ISO timestamp
    user_id: int  # Кому отправлялось


# ============================================
# VIDEO CACHE
# ============================================

class VideoCache:
    """
    Кэш отправленных видео для предотвращения повторов.
    Сохраняется в JSON файл для персистентности.
    """

    def __init__(self, cache_file: str):
        self.cache_file = Path(cache_file)
        self._cache: Dict[str, CachedVideo] = {}  # video_id -> CachedVideo
        self._user_videos: Dict[int, set] = {}  # user_id -> set of video_ids
        self._load_cache()

    def _load_cache(self):
        """Загрузить кэш из файла"""
        try:
            if self.cache_file.exists():
                import json
                with open(self.cache_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)

                for video_id, video_data in data.get("videos", {}).items():
                    self._cache[video_id] = CachedVideo(**video_data)

                    user_id = video_data.get("user_id", 0)
                    if user_id not in self._user_videos:
                        self._user_videos[user_id] = set()
                    self._user_videos[user_id].add(video_id)

                logger.info(f"Loaded {len(self._cache)} videos from cache")
        except Exception as e:
            logger.warning(f"Failed to load cache: {e}")

    def _save_cache(self):
        """Сохранить кэш в файл"""
        try:
            import json
            data = {
                "videos": {
                    vid: {
                        "video_id": v.video_id,
                        "video_url": v.video_url,
                        "author_id": v.author_id,
                        "title": v.title,
                        "file_id": v.file_id,
                        "cached_at": v.cached_at,
                        "user_id": v.user_id
                    }
                    for vid, v in self._cache.items()
                }
            }
            with open(self.cache_file, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"Failed to save cache: {e}")

    def add(self, video_id: str, video_url: str, author_id: str, title: str,
            file_id: str, user_id: int):
        """Добавить видео в кэш"""
        cached = CachedVideo(
            video_id=video_id,
            video_url=video_url,
            author_id=author_id,
            title=title,
            file_id=file_id,
            cached_at=datetime.now().isoformat(),
            user_id=user_id
        )
        self._cache[video_id] = cached

        if user_id not in self._user_videos:
            self._user_videos[user_id] = set()
        self._user_videos[user_id].add(video_id)

        self._save_cache()
        logger.debug(f"Cached video {video_id} for user {user_id}")

    def is_sent_to_user(self, video_id: str, user_id: int) -> bool:
        """Проверить, было ли видео отправлено пользователю"""
        if user_id not in self._user_videos:
            return False
        return video_id in self._user_videos[user_id]

    def get_file_id(self, video_id: str) -> Optional[str]:
        """Получить file_id из кэша"""
        cached = self._cache.get(video_id)
        return cached.file_id if cached else None

    def filter_new_videos(self, videos: List[Dict[str, Any]], user_id: int) -> List[Dict[str, Any]]:
        """Отфильтровать уже отправленные пользователю видео"""
        return [v for v in videos if not self.is_sent_to_user(v.get("video_id", ""), user_id)]

    def get_stats(self, user_id: int) -> Dict[str, int]:
        """Получить статистику кэша для пользователя"""
        user_count = len(self._user_videos.get(user_id, set()))
        return {
            "total": len(self._cache),
            "user": user_count
        }


# ============================================
# VIDEO UNIQUEIZER (FFmpeg)
# ============================================

class VideoUniqueizer:
    """
    Уникализация видео через FFmpeg

    Новый принцип:
    - Наложение 2 случайных overlay изображений с прозрачностью 0.1%-1%
    - Опционально: размытый фон + смещение видео на 10-40px
    - Каждый раз разные метаданные
    """

    def __init__(self, temp_dir: str, ffmpeg_path: str = "ffmpeg", overlays_dir: str = None):
        self.temp_dir = Path(temp_dir)
        self.temp_dir.mkdir(parents=True, exist_ok=True)
        self.ffmpeg_path = ffmpeg_path
        self.overlays_dir = Path(overlays_dir) if overlays_dir else Path(Config.OVERLAYS_DIR)
        self.overlays_dir.mkdir(parents=True, exist_ok=True)
        self._semaphore = asyncio.Semaphore(Config.MAX_CONCURRENT_FFMPEG)
        self._overlay_images: List[Path] = []
        self._load_overlays()

    def _load_overlays(self):
        """Загрузить список overlay изображений"""
        image_extensions = {'.png', '.jpg', '.jpeg', '.bmp', '.webp'}
        self._overlay_images = [
            f for f in self.overlays_dir.iterdir()
            if f.is_file() and f.suffix.lower() in image_extensions
        ]
        logger.info(f"Loaded {len(self._overlay_images)} overlay images from {self.overlays_dir}")

        if len(self._overlay_images) < 2:
            logger.warning(
                f"Need at least 2 overlay images in {self.overlays_dir}. "
                f"Currently have {len(self._overlay_images)}. "
                f"Overlay feature will be disabled."
            )

    def _get_random_overlays(self, count: int = 2) -> List[Path]:
        """Получить случайные overlay изображения"""
        if len(self._overlay_images) < count:
            return []
        return random.sample(self._overlay_images, count)

    def _generate_random_metadata(self) -> Dict[str, str]:
        """
        Генерация метаданных как у видеоредакторов CapCut/VN.
        Эти метаданные делают видео похожим на экспорт из мобильных редакторов.
        """
        # Случайная дата создания (последние 30 дней)
        days_ago = random.randint(0, 30)
        hours_ago = random.randint(0, 23)
        random_date = datetime.now() - timedelta(days=days_ago, hours=hours_ago)
        creation_time = random_date.strftime("%Y-%m-%dT%H:%M:%S.000000Z")

        # Варианты редакторов и их метаданных
        editors = [
            {
                # CapCut стиль
                "encoder": "CapCut",
                "handler_name": "CapCut Video Handler",
                "vendor_id": "[0][0][0][0]",
                "compatible_brands": "isomiso2avc1mp41",
                "major_brand": "isom",
                "minor_version": "512",
                "comment": f"CapCut {random.choice(['3.9.0', '4.0.0', '4.1.0', '4.2.0', '4.3.0'])}",
            },
            {
                # VN Video Editor стиль
                "encoder": "VN Video Editor",
                "handler_name": "VN Media Handler",
                "vendor_id": "[0][0][0][0]",
                "compatible_brands": "isomiso2avc1mp41",
                "major_brand": "isom",
                "minor_version": "512",
                "comment": f"VN {random.choice(['1.40.8', '1.41.0', '1.42.0', '2.0.0', '2.1.0'])}",
            },
            {
                # InShot стиль
                "encoder": "InShot Video Editor",
                "handler_name": "InShot Handler",
                "vendor_id": "[0][0][0][0]",
                "compatible_brands": "isomiso2avc1mp41",
                "major_brand": "isom",
                "minor_version": "512",
                "comment": f"InShot {random.choice(['1.920.1389', '1.930.1400', '1.940.1410'])}",
            },
            {
                # Kinemaster стиль
                "encoder": "Kinemaster",
                "handler_name": "Kinemaster Video Handler",
                "vendor_id": "[0][0][0][0]",
                "compatible_brands": "mp42isom",
                "major_brand": "mp42",
                "minor_version": "0",
                "comment": f"Kinemaster {random.choice(['6.0.0', '6.1.0', '6.2.0', '7.0.0'])}",
            },
        ]

        editor = random.choice(editors)

        # Случайный ID устройства (как у мобильных устройств)
        device_id = ''.join(random.choices('0123456789abcdef', k=16))

        return {
            "title": "",  # Пустой title как у мобильных редакторов
            "artist": "",
            "album": "",
            "comment": editor["comment"],
            "creation_time": creation_time,
            "encoder": editor["encoder"],
            "handler_name": editor["handler_name"],
            "compatible_brands": editor["compatible_brands"],
            "major_brand": editor["major_brand"],
            "minor_version": editor["minor_version"],
        }

    async def uniqueize(
        self,
        input_path: str,
        output_path: Optional[str] = None,
        use_blur_background: bool = None,
        use_snow_effect: bool = False
    ) -> Optional[str]:
        """
        Уникализировать видео

        Args:
            input_path: Путь к исходному видео
            output_path: Путь для сохранения (если None - генерируется)
            use_blur_background: Использовать размытый фон (None = из Config)
            use_snow_effect: Добавить эффект снега (плавающая точка)

        Returns:
            Путь к уникализированному видео или None при ошибке
        """
        async with self._semaphore:
            return await self._process_video(input_path, output_path, use_blur_background, use_snow_effect)

    async def _process_video(
        self,
        input_path: str,
        output_path: Optional[str] = None,
        use_blur_background: bool = None,
        use_snow_effect: bool = False
    ) -> Optional[str]:
        """Внутренняя обработка видео"""
        if not output_path:
            output_path = str(self.temp_dir / f"unique_{uuid.uuid4().hex[:8]}.mp4")

        if use_blur_background is None:
            use_blur_background = Config.USE_BLUR_BACKGROUND

        # Получаем overlay изображения
        overlays = self._get_random_overlays(2)

        # Генерируем параметры
        metadata = self._generate_random_metadata()
        overlay_opacity = [random.uniform(0.001, 0.01) for _ in range(2)]  # 0.1% - 1%
        video_offset = random.randint(10, 40) if use_blur_background else 0

        # Формируем FFmpeg команду
        cmd = self._build_ffmpeg_command(
            input_path=input_path,
            output_path=output_path,
            overlays=overlays,
            overlay_opacity=overlay_opacity,
            metadata=metadata,
            use_blur_background=use_blur_background,
            video_offset=video_offset,
            use_snow_effect=use_snow_effect
        )

        logger.info(f"FFmpeg processing: {input_path} -> {output_path}")
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
                logger.error(f"FFmpeg failed with code {process.returncode}")
                logger.error(f"FFmpeg stderr: {stderr.decode()}")
                return None

            # Проверяем что файл создан
            if os.path.exists(output_path):
                file_size = os.path.getsize(output_path)
                if file_size > 0:
                    logger.info(f"FFmpeg success: created {output_path} ({file_size} bytes)")
                    return output_path
                else:
                    logger.error(f"FFmpeg created empty file: {output_path}")
                    return None
            else:
                logger.error(f"FFmpeg did not create output file: {output_path}")
                return None

        except asyncio.TimeoutError:
            logger.error("FFmpeg timeout (5 min)")
            return None
        except Exception as e:
            logger.error(f"FFmpeg exception: {e}")
            import traceback
            logger.error(traceback.format_exc())
            return None

    def _build_ffmpeg_command(
        self,
        input_path: str,
        output_path: str,
        overlays: List[Path],
        overlay_opacity: List[float],
        metadata: Dict[str, str],
        use_blur_background: bool,
        video_offset: int,
        use_snow_effect: bool = False
    ) -> List[str]:
        """Построение FFmpeg команды"""

        cmd = [self.ffmpeg_path, "-y"]

        # Входной файл (видео)
        cmd.extend(["-i", input_path])

        # Добавляем overlay изображения
        for overlay in overlays:
            cmd.extend(["-i", str(overlay)])

        # Строим filter_complex
        filter_parts = []

        if use_blur_background:
            # Размытый фон: масштабируем видео, размываем, накладываем оригинал со смещением
            # [0:v] - исходное видео
            # Создаем размытый фон
            filter_parts.append(
                f"[0:v]scale=iw+{video_offset*2}:ih+{video_offset*2},boxblur=20:5[bg]"
            )
            # Накладываем оригинал со смещением
            filter_parts.append(
                f"[bg][0:v]overlay={video_offset}:{video_offset}[base]"
            )
            base_label = "[base]"
        else:
            # Без размытого фона - просто копируем
            filter_parts.append("[0:v]null[base]")
            base_label = "[base]"

        # Накладываем overlay изображения
        current_label = base_label
        for i, (overlay, opacity) in enumerate(zip(overlays, overlay_opacity)):
            next_label = f"[v{i}]"
            # Масштабируем overlay под размер видео и применяем прозрачность
            filter_parts.append(
                f"[{i+1}:v]scale=iw:ih,format=rgba,"
                f"colorchannelmixer=aa={opacity:.4f}[ovr{i}]"
            )
            # Накладываем
            filter_parts.append(
                f"{current_label}[ovr{i}]overlay=0:0:format=auto{next_label}"
            )
            current_label = next_label

        # Добавляем эффект снега (маленькая белая точка на видео)
        if use_snow_effect:
            snow_label = current_label.strip("[]")
            # Точка в случайной позиции, достаточно большая чтобы быть заметной
            # Позиция в процентах от размера видео для совместимости
            dot_x_pct = random.randint(5, 25)  # 5-25% от ширины
            dot_y_pct = random.randint(5, 20)  # 5-20% от высоты
            dot_size = random.randint(5, 8)  # Размер 5-8 пикселей (видимый)
            # drawbox с позицией в процентах от размера
            filter_parts.append(
                f"[{snow_label}]drawbox=x=iw*{dot_x_pct}/100:y=ih*{dot_y_pct}/100:"
                f"w={dot_size}:h={dot_size}:color=white@0.10:t=fill[snow]"
            )
            current_label = "[snow]"

        # Дополнительная уникализация: цветокоррекция (незаметная)
        final_label = current_label.strip("[]")

        # Случайные микро-изменения цвета (незаметны глазу, но меняют хэш)
        hue_shift = random.uniform(-2, 2)  # Сдвиг оттенка ±2 градуса
        saturation = random.uniform(0.98, 1.02)  # Насыщенность ±2%
        brightness = random.uniform(-0.02, 0.02)  # Яркость ±2%
        contrast = random.uniform(0.98, 1.02)  # Контраст ±2%

        # Добавляем цветокоррекцию и финальное масштабирование
        filter_parts.append(
            f"[{final_label}]hue=h={hue_shift:.2f}:s={saturation:.3f},"
            f"eq=brightness={brightness:.3f}:contrast={contrast:.3f},"
            f"scale=trunc(iw/2)*2:trunc(ih/2)*2[out]"
        )

        # Если нет overlay - упрощенный фильтр
        if not overlays:
            snow_filter = ""
            if use_snow_effect:
                dot_x_pct = random.randint(5, 25)
                dot_y_pct = random.randint(5, 20)
                dot_size = random.randint(5, 8)
                snow_filter = (
                    f",drawbox=x=iw*{dot_x_pct}/100:y=ih*{dot_y_pct}/100:"
                    f"w={dot_size}:h={dot_size}:color=white@0.10:t=fill"
                )

            # Цветокоррекция для случая без overlay
            color_correction = (
                f",hue=h={hue_shift:.2f}:s={saturation:.3f},"
                f"eq=brightness={brightness:.3f}:contrast={contrast:.3f}"
            )

            if use_blur_background:
                filter_complex = (
                    f"[0:v]scale=iw+{video_offset*2}:ih+{video_offset*2},"
                    f"boxblur=20:5[bg];"
                    f"[bg][0:v]overlay={video_offset}:{video_offset}{snow_filter}{color_correction},"
                    f"scale=trunc(iw/2)*2:trunc(ih/2)*2[out]"
                )
            else:
                # Без размытого фона - базовые фильтры
                base_filters = []
                if snow_filter:
                    base_filters.append(snow_filter.lstrip(','))
                base_filters.append(color_correction.lstrip(','))
                base_filters.append("scale=trunc(iw/2)*2:trunc(ih/2)*2")
                filter_complex = f"[0:v]{','.join(base_filters)}[out]"
        else:
            filter_complex = ";".join(filter_parts)

        cmd.extend(["-filter_complex", filter_complex])
        cmd.extend(["-map", "[out]", "-map", "0:a?"])

        # Видеокодек с рандомными параметрами для уникальности
        crf = random.randint(20, 25)  # Случайное качество
        cmd.extend([
            "-c:v", "libx264",
            "-preset", random.choice(["fast", "medium"]),
            "-crf", str(crf),
            "-b:v", random.choice(["2M", "2.5M", "3M", "3.5M", "4M"]),
            "-pix_fmt", "yuv420p",  # Стандартный формат пикселей
        ])

        # Аудиокодек с рандомными параметрами
        cmd.extend([
            "-c:a", "aac",
            "-b:a", random.choice(["128k", "160k", "192k", "224k"]),
            "-ar", random.choice(["44100", "48000"]),  # Случайная частота дискретизации
        ])

        # Метаданные (очистка старых + новые случайные)
        cmd.extend(["-map_metadata", "-1"])
        for key, value in metadata.items():
            cmd.extend(["-metadata", f"{key}={value}"])

        # Оптимизация для веба
        cmd.extend(["-movflags", "+faststart"])

        # Ограничение потоков для снижения нагрузки
        cmd.extend(["-threads", "2"])

        cmd.append(output_path)

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

                        # Получаем время создания видео
                        create_time = v.get("create_time", 0)
                        video_year = None
                        if create_time:
                            try:
                                video_year = datetime.fromtimestamp(create_time).year
                            except Exception:
                                pass

                        videos.append({
                            "video_url": v.get("play", ""),
                            "author": v.get("author", {}).get("nickname", "Unknown"),
                            "author_id": v.get("author", {}).get("unique_id", ""),
                            "title": v.get("title", ""),
                            "play_count": v.get("play_count", 0),
                            "like_count": v.get("digg_count", 0),
                            "duration": duration,
                            "video_id": v.get("video_id", ""),
                            "create_time": create_time,
                            "year": video_year,
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

    async def search_videos_batch(
        self,
        query: str,
        count: int = 30,
        cursor: int = 0
    ) -> Tuple[List[Dict[str, Any]], int, bool]:
        """
        Поиск видео с поддержкой пагинации.
        Возвращает (videos, next_cursor, has_more)
        """
        session = await self._get_session()
        query = query.lstrip('#')
        videos = []
        next_cursor = cursor
        has_more = False

        try:
            async with session.post(
                self.TIKWM_FEED_API,
                data={"keywords": query, "count": count, "cursor": cursor, "hd": 1}
            ) as response:
                if response.status != 200:
                    return videos, next_cursor, False

                data = await response.json()
                if data.get("code") != 0:
                    return videos, next_cursor, False

                batch_videos = data.get("data", {}).get("videos", [])
                if not batch_videos:
                    return videos, next_cursor, False

                for v in batch_videos:
                    duration = v.get("duration", 0)
                    if duration > Config.MAX_VIDEO_DURATION:
                        continue

                    create_time = v.get("create_time", 0)
                    video_year = None
                    if create_time:
                        try:
                            video_year = datetime.fromtimestamp(create_time).year
                        except Exception:
                            pass

                    videos.append({
                        "video_url": v.get("play", ""),
                        "author": v.get("author", {}).get("nickname", "Unknown"),
                        "author_id": v.get("author", {}).get("unique_id", ""),
                        "title": v.get("title", ""),
                        "play_count": v.get("play_count", 0),
                        "like_count": v.get("digg_count", 0),
                        "duration": duration,
                        "video_id": v.get("video_id", ""),
                        "create_time": create_time,
                        "year": video_year,
                    })

                next_cursor = data.get("data", {}).get("cursor", cursor + count)
                has_more = data.get("data", {}).get("hasMore", False)

        except Exception as e:
            logger.error(f"Error searching videos batch: {e}")

        return videos, next_cursor, has_more

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

    def __init__(self, bot: Bot, downloader: TikTokDownloader, uniqueizer: VideoUniqueizer,
                 storage_chat_id: int, video_cache: VideoCache, user_settings: 'UserSettings' = None):
        self.bot = bot
        self.downloader = downloader
        self.uniqueizer = uniqueizer
        self.storage_chat_id = storage_chat_id
        self.video_cache = video_cache
        self.user_settings = user_settings
        self._user_queues: Dict[int, UserQueue] = {}
        self._processing_tasks: Dict[int, asyncio.Task] = {}
        self._delete_tasks: List[asyncio.Task] = []  # Задачи на удаление из storage
        self._last_send_time: float = 0  # Время последней отправки

    async def _send_document_with_retry(
        self,
        chat_id: int,
        document: Any,
        caption: str = None,
        reply_markup: Any = None,
        disable_notification: bool = False,
        filename: str = None,
        disable_content_type_detection: bool = False
    ) -> Optional[Message]:
        """
        Отправить документ (файл) с обработкой flood control.
        Видео отправляется как файл, чтобы сохранить качество и метаданные.
        """
        # Соблюдаем минимальную задержку между отправками
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_send_time
        if elapsed < Config.SEND_DELAY:
            await asyncio.sleep(Config.SEND_DELAY - elapsed)

        for attempt in range(Config.MAX_RETRY_ATTEMPTS):
            try:
                # Если это FSInputFile, можно задать имя файла
                if filename and hasattr(document, 'path'):
                    document = FSInputFile(document.path, filename=filename)

                result = await self.bot.send_document(
                    chat_id=chat_id,
                    document=document,
                    caption=caption,
                    reply_markup=reply_markup,
                    disable_notification=disable_notification,
                    disable_content_type_detection=disable_content_type_detection
                )
                self._last_send_time = asyncio.get_event_loop().time()
                return result

            except TelegramRetryAfter as e:
                wait_time = e.retry_after + 1  # +1 секунда для надежности
                logger.warning(f"Flood control: waiting {wait_time}s (attempt {attempt + 1}/{Config.MAX_RETRY_ATTEMPTS})")
                await asyncio.sleep(wait_time)

            except Exception as e:
                logger.error(f"Error sending document (attempt {attempt + 1}/{Config.MAX_RETRY_ATTEMPTS}): {e}")
                import traceback
                logger.error(f"Traceback: {traceback.format_exc()}")
                if attempt == Config.MAX_RETRY_ATTEMPTS - 1:
                    return None
                await asyncio.sleep(2 ** attempt)  # Exponential backoff

        return None

    async def _send_video_with_retry(
        self,
        chat_id: int,
        video: Any,
        caption: str = None,
        reply_markup: Any = None,
        supports_streaming: bool = True,
        disable_notification: bool = False
    ) -> Optional[Message]:
        """
        Отправить видео с обработкой flood control.
        Для спаршенных видео (без уникализации).
        """
        # Соблюдаем минимальную задержку между отправками
        now = asyncio.get_event_loop().time()
        elapsed = now - self._last_send_time
        if elapsed < Config.SEND_DELAY:
            await asyncio.sleep(Config.SEND_DELAY - elapsed)

        for attempt in range(Config.MAX_RETRY_ATTEMPTS):
            try:
                result = await self.bot.send_video(
                    chat_id=chat_id,
                    video=video,
                    caption=caption,
                    reply_markup=reply_markup,
                    supports_streaming=supports_streaming,
                    disable_notification=disable_notification
                )
                self._last_send_time = asyncio.get_event_loop().time()
                return result

            except TelegramRetryAfter as e:
                wait_time = e.retry_after + 1
                logger.warning(f"Flood control: waiting {wait_time}s (attempt {attempt + 1}/{Config.MAX_RETRY_ATTEMPTS})")
                await asyncio.sleep(wait_time)

            except Exception as e:
                logger.error(f"Error sending video: {e}")
                if attempt == Config.MAX_RETRY_ATTEMPTS - 1:
                    return None
                await asyncio.sleep(2 ** attempt)

        return None

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
        video_id = task.video_id or str(uuid.uuid4())[:12]

        try:
            # Режим: только уникализация из очереди (file_id уже есть)
            if task.do_uniqueize and task.file_id:
                await self._process_uniqueize_only(task)
                return

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

            # Режим: только скачивание (для парсера - без уникализации)
            if task.skip_uniqueization:
                await self._process_download_only(task, downloaded_path, video_id)
                return

            # Шаг 2: Уникализация (стандартный режим)
            task.status = "processing"
            unique_path = await self.uniqueizer.uniqueize(
                downloaded_path,
                use_blur_background=task.use_blur_background,
                use_snow_effect=task.use_snow_effect
            )

            if not unique_path:
                task.status = "error"
                task.error_message = "Не удалось обработать видео"
                await self._send_error(task)
                return

            # Шаг 3: Отправка в storage группу с хэштегами
            task.status = "sending"

            # Формируем caption для storage с хэштегами
            author_id = task.author_id or 'unknown'
            storage_caption = (
                f"#vid #cache\n"
                f"📹 ID: {video_id}\n"
                f"👤 Author: @{author_id}\n"
                f"🔗 URL: {task.video_url or 'file'}\n"
                f"👥 User: {task.user_id}"
            )

            # Генерируем имя файла как у редактора
            filename = f"CapCut_{random.randint(1000000, 9999999)}.mp4"

            # Проверяем что файл существует
            if not os.path.exists(unique_path):
                logger.error(f"[Task] Uniqueized file does not exist: {unique_path}")
                task.status = "error"
                task.error_message = "Файл не был создан"
                await self._send_error(task)
                return

            file_size = os.path.getsize(unique_path)
            logger.info(f"[Task] Sending file: {unique_path}, size: {file_size} bytes, to storage: {self.storage_chat_id}")

            video_file = FSInputFile(unique_path, filename=filename)

            # Отправляем в storage группу как файл (с retry при flood control)
            storage_msg = await self._send_document_with_retry(
                chat_id=self.storage_chat_id,
                document=video_file,
                caption=storage_caption,
                disable_notification=True
            )

            # Telegram может вернуть document или video в зависимости от файла
            if not storage_msg:
                logger.error(f"[Task] Failed to send to storage - no response")
                task.status = "error"
                task.error_message = "Не удалось отправить видео в storage"
                await self._send_error(task)
                return

            # Получаем file_id (может быть document или video)
            if storage_msg.document:
                file_id = storage_msg.document.file_id
            elif storage_msg.video:
                file_id = storage_msg.video.file_id
            else:
                logger.error(f"[Task] No document or video in response")
                task.status = "error"
                task.error_message = "Не удалось получить file_id"
                await self._send_error(task)
                return

            logger.info(f"[Task] Sent to storage, file_id={file_id[:30]}...")

            # Добавляем в кэш
            self.video_cache.add(
                video_id=video_id,
                video_url=task.video_url or "",
                author_id=author_id,
                title=task.title,
                file_id=file_id,
                user_id=task.user_id
            )

            # Отправляем пользователю ФАЙЛ напрямую (без превью видео!)
            user_file = FSInputFile(unique_path, filename=filename)
            await self._send_document_with_retry(
                chat_id=task.chat_id,
                document=user_file,
                caption=None,
                disable_content_type_detection=True  # Отправить как чистый файл без превью!
            )

            # Планируем удаление из storage через 10 минут
            delete_task = asyncio.create_task(
                self._delete_from_storage_delayed(
                    self.storage_chat_id,
                    storage_msg.message_id,
                    Config.STORAGE_VIDEO_TTL
                )
            )
            self._delete_tasks.append(delete_task)

            task.status = "done"
            logger.info(f"Task {task.task_id} completed successfully, cached as {video_id}")

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

    async def _process_download_only(self, task: VideoTask, downloaded_path: str, video_id: str):
        """Обработка только скачивания (без уникализации) - для парсера. Видео отправляется как видео."""
        try:
            task.status = "sending"

            # Формируем caption для storage с хэштегами
            author_id = task.author_id or 'unknown'
            storage_caption = (
                f"#vid #download\n"
                f"📹 ID: {video_id}\n"
                f"👤 Author: @{author_id}\n"
                f"🔗 URL: {task.video_url or 'file'}\n"
                f"👥 User: {task.user_id}"
            )

            video_file = FSInputFile(downloaded_path)

            # Отправляем в storage группу как ВИДЕО (с retry при flood control)
            storage_msg = await self._send_video_with_retry(
                chat_id=self.storage_chat_id,
                video=video_file,
                caption=storage_caption,
                supports_streaming=True,
                disable_notification=True
            )

            if not storage_msg or not storage_msg.video:
                task.status = "error"
                task.error_message = "Не удалось отправить видео в storage"
                await self._send_error(task)
                return

            file_id = storage_msg.video.file_id

            # Добавляем в кэш
            self.video_cache.add(
                video_id=video_id,
                video_url=task.video_url or "",
                author_id=author_id,
                title=task.title,
                file_id=file_id,
                user_id=task.user_id
            )

            # Формируем caption для пользователя с предложением уникализации
            user_caption_parts = []
            if task.author:
                user_caption_parts.append(f"🎬 {task.author}")
            if task.title:
                user_caption_parts.append(task.title[:200])
            user_caption_parts.append("\n📥 Скачано без водяного знака")
            user_caption_parts.append("➡️ Добавить в список уникализаций?")
            user_caption = "\n".join(user_caption_parts)

            # Инлайн кнопки для добавления в очередь уникализации
            builder = InlineKeyboardBuilder()
            # Сохраняем file_id и метаданные в callback_data (ограничение 64 байта)
            short_file_id = file_id[:40]  # Урезаем file_id
            builder.button(text="✅ Да, уникализировать", callback_data=f"uq_add:{short_file_id}")
            builder.button(text="❌ Нет", callback_data=f"uq_skip:{short_file_id}")
            builder.adjust(1)

            # Отправляем пользователю как ВИДЕО с кнопками (с retry при flood control)
            sent_msg = await self._send_video_with_retry(
                chat_id=task.chat_id,
                video=file_id,
                caption=user_caption,
                reply_markup=builder.as_markup(),
                supports_streaming=True
            )

            if not sent_msg:
                task.status = "error"
                task.error_message = "Не удалось отправить видео пользователю"
                await self._send_error(task)
                return

            # Сохраняем данные видео для callback (через user_settings)
            if self.user_settings:
                # Временное хранение данных видео по short_file_id
                video_data = {
                    "file_id": file_id,
                    "author": task.author,
                    "author_id": author_id,
                    "title": task.title,
                    "video_id": video_id,
                    "message_id": sent_msg.message_id
                }
                pending_key = f"pending_video_{short_file_id}"
                self.user_settings.set(task.user_id, pending_key, video_data)

            # Планируем удаление из storage через 10 минут
            delete_task = asyncio.create_task(
                self._delete_from_storage_delayed(
                    self.storage_chat_id,
                    storage_msg.message_id,
                    Config.STORAGE_VIDEO_TTL
                )
            )
            self._delete_tasks.append(delete_task)

            task.status = "done"
            logger.info(f"Task {task.task_id} downloaded (no unique), cached as {video_id}")

        except Exception as e:
            logger.error(f"Error in download-only task {task.task_id}: {e}")
            task.status = "error"
            task.error_message = str(e)
            await self._send_error(task)
        finally:
            # Очистка скачанного файла
            await self.uniqueizer.cleanup_file(downloaded_path)

    async def _process_uniqueize_only(self, task: VideoTask):
        """Обработка только уникализации (видео уже в Telegram)"""
        downloaded_path = None
        unique_path = None

        try:
            task.status = "downloading"
            logger.info(f"[Uniqueize] Starting task {task.task_id}, file_id={task.file_id[:20] if task.file_id else 'None'}...")

            # Скачиваем файл из Telegram
            downloaded_path = str(self.uniqueizer.temp_dir / f"tg_{uuid.uuid4().hex[:8]}.mp4")
            file = await self.bot.get_file(task.file_id)
            logger.info(f"[Uniqueize] Downloading from Telegram: {file.file_path}")
            await self.bot.download_file(file.file_path, downloaded_path)

            if os.path.exists(downloaded_path):
                logger.info(f"[Uniqueize] Downloaded: {downloaded_path}, size: {os.path.getsize(downloaded_path)} bytes")
            else:
                logger.error(f"[Uniqueize] Failed to download file to {downloaded_path}")
                task.status = "error"
                task.error_message = "Не удалось скачать файл из Telegram"
                await self._send_error(task)
                return

            # Уникализация
            task.status = "processing"
            logger.info(f"[Uniqueize] Processing with blur={task.use_blur_background}, snow={task.use_snow_effect}")
            unique_path = await self.uniqueizer.uniqueize(
                downloaded_path,
                use_blur_background=task.use_blur_background,
                use_snow_effect=task.use_snow_effect
            )

            if not unique_path:
                logger.error(f"[Uniqueize] FFmpeg failed for task {task.task_id}")
                task.status = "error"
                task.error_message = "Не удалось обработать видео"
                await self._send_error(task)
                return

            task.status = "sending"

            # Формируем caption для storage
            video_id = task.video_id or str(uuid.uuid4())[:12]
            author_id = task.author_id or 'unknown'
            storage_caption = (
                f"#vid #unique\n"
                f"📹 ID: {video_id}\n"
                f"👤 Author: @{author_id}\n"
                f"👥 User: {task.user_id}"
            )

            # Генерируем имя файла как у редактора
            filename = f"CapCut_{random.randint(1000000, 9999999)}.mp4"

            # Проверяем что файл существует
            if not os.path.exists(unique_path):
                logger.error(f"Uniqueized file does not exist: {unique_path}")
                task.status = "error"
                task.error_message = "Файл не был создан"
                await self._send_error(task)
                return

            file_size = os.path.getsize(unique_path)
            logger.info(f"[Uniqueize] Sending file: {unique_path}, size: {file_size} bytes, to storage: {self.storage_chat_id}")

            video_file = FSInputFile(unique_path, filename=filename)

            # Отправляем в storage группу как файл (с retry при flood control)
            storage_msg = await self._send_document_with_retry(
                chat_id=self.storage_chat_id,
                document=video_file,
                caption=storage_caption,
                disable_notification=True
            )

            # Telegram может вернуть document или video в зависимости от файла
            if not storage_msg:
                logger.error(f"[Uniqueize] Failed to send to storage - no response")
                task.status = "error"
                task.error_message = "Не удалось отправить видео в storage"
                await self._send_error(task)
                return

            # Получаем file_id (может быть document или video)
            if storage_msg.document:
                unique_file_id = storage_msg.document.file_id
            elif storage_msg.video:
                unique_file_id = storage_msg.video.file_id
            else:
                logger.error(f"[Uniqueize] No document or video in response")
                task.status = "error"
                task.error_message = "Не удалось получить file_id"
                await self._send_error(task)
                return

            logger.info(f"[Uniqueize] Sent to storage, file_id={unique_file_id[:30]}...")

            # Отправляем пользователю ФАЙЛ напрямую (без превью видео!)
            user_file = FSInputFile(unique_path, filename=filename)
            await self._send_document_with_retry(
                chat_id=task.chat_id,
                document=user_file,
                caption=None,
                disable_content_type_detection=True  # Отправить как чистый файл без превью!
            )

            # Планируем удаление из storage
            delete_task = asyncio.create_task(
                self._delete_from_storage_delayed(
                    self.storage_chat_id,
                    storage_msg.message_id,
                    Config.STORAGE_VIDEO_TTL
                )
            )
            self._delete_tasks.append(delete_task)

            task.status = "done"
            logger.info(f"Task {task.task_id} uniqueized successfully")

        except Exception as e:
            logger.error(f"Error in uniqueize-only task {task.task_id}: {e}")
            task.status = "error"
            task.error_message = str(e)
            await self._send_error(task)

        finally:
            if downloaded_path:
                await self.uniqueizer.cleanup_file(downloaded_path)
            if unique_path:
                await self.uniqueizer.cleanup_file(unique_path)

    async def _delete_from_storage_delayed(self, chat_id: int, message_id: int, delay: int):
        """Удалить сообщение из storage через delay секунд"""
        try:
            await asyncio.sleep(delay)
            await self.bot.delete_message(chat_id=chat_id, message_id=message_id)
            logger.debug(f"Deleted message {message_id} from storage after {delay}s")
        except Exception as e:
            logger.warning(f"Failed to delete message {message_id} from storage: {e}")

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
        KeyboardButton(text="⚙️ Настройки")
    )
    builder.row(
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

    def __init__(self, token: str, storage_chat_id: int, ffmpeg_path: str = "ffmpeg"):
        self.bot = Bot(token=token)
        self.dp = Dispatcher(storage=MemoryStorage())
        self.router = Router()
        self.storage_chat_id = storage_chat_id

        # Компоненты
        self.downloader = TikTokDownloader(Config.TEMP_DIR)
        self.uniqueizer = VideoUniqueizer(Config.TEMP_DIR, ffmpeg_path)
        self.video_cache = VideoCache(Config.CACHE_FILE)
        self.user_settings = UserSettings()
        self.queue_manager = QueueManager(
            self.bot, self.downloader, self.uniqueizer,
            storage_chat_id, self.video_cache, self.user_settings
        )

        # Настройка хэндлеров
        self._setup_handlers()
        self.dp.include_router(self.router)

    def _setup_handlers(self):
        """Настройка всех хэндлеров"""

        # /start
        @self.router.message(CommandStart())
        async def cmd_start(message: Message, state: FSMContext):
            await state.clear()
            queue_count = self.user_settings.get_unique_queue_count(message.from_user.id)
            queue_info = f"\n\n📋 В очереди на уникализацию: {queue_count}" if queue_count > 0 else ""

            await message.answer(
                "👋 **Привет!**\n\n"
                "Я бот для парсинга и уникализации видео с TikTok.\n\n"
                "**Как пользоваться:**\n"
                "1️⃣ 🎬 Парсер - скачать видео по запросу\n"
                "2️⃣ Выбрать какие видео уникализировать\n"
                "3️⃣ 🔄 Уникализация - обработать выбранные\n\n"
                f"Выберите действие:{queue_info}",
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
            queue_count = self.user_settings.get_unique_queue_count(message.from_user.id)

            builder = InlineKeyboardBuilder()
            if queue_count > 0:
                builder.button(
                    text=f"🚀 Уникализировать все ({queue_count} видео)",
                    callback_data="uniqueize_all"
                )
                builder.button(
                    text="🗑️ Очистить очередь",
                    callback_data="clear_unique_queue"
                )
            builder.button(text="📤 Отправить видео вручную", callback_data="send_video_manual")
            builder.button(text="❌ Отмена", callback_data="cancel")
            builder.adjust(1)

            queue_info = ""
            if queue_count > 0:
                queue_info = f"\n\n📋 **В очереди на уникализацию:** {queue_count} видео"

            await message.answer(
                "🔄 **Уникализация видео**\n\n"
                "Здесь вы можете:\n"
                "• Уникализировать видео из очереди парсера\n"
                "• Отправить свое видео вручную\n\n"
                "Видео будет обработано и возвращено с изменениями,\n"
                "невидимыми для глаза, но уникальными для соцсетей."
                f"{queue_info}",
                reply_markup=builder.as_markup(),
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

        # Вспомогательная функция для генерации настроек
        def _get_settings_text_and_keyboard(user_id: int):
            blur_enabled = self.user_settings.get_blur_background(user_id)
            blur_status = "✅ Вкл" if blur_enabled else "❌ Выкл"

            snow_enabled = self.user_settings.get_snow_effect(user_id)
            snow_status = "✅ Вкл" if snow_enabled else "❌ Выкл"

            year_filter = self.user_settings.get_year_filter(user_id)
            year_mode = year_filter.get("mode", "off")
            year_value = year_filter.get("year")

            if year_mode == "off":
                year_status = "❌ Выключен"
            elif year_mode == "only":
                year_status = f"✅ Только {year_value}"
            elif year_mode == "exclude":
                year_status = f"🚫 Исключить {year_value}"
            else:
                year_status = "❌ Выключен"

            builder = InlineKeyboardBuilder()
            builder.button(
                text=f"{'🔵' if blur_enabled else '⚪'} Размытый фон: {blur_status}",
                callback_data="toggle_blur"
            )
            builder.button(
                text=f"{'❄️' if snow_enabled else '⚪'} Снег (точка): {snow_status}",
                callback_data="toggle_snow"
            )
            builder.button(
                text=f"📅 Фильтр по году: {year_status}",
                callback_data="year_filter_menu"
            )
            builder.button(text="◀️ Назад", callback_data="back_to_menu")
            builder.adjust(1)

            overlays_count = len(self.uniqueizer._overlay_images)

            text = (
                "⚙️ **Настройки**\n\n"
                f"**Размытый фон:** {blur_status}\n"
                "При включении видео будет со смещением на размытом фоне\n\n"
                f"**Снег (точка):** {snow_status}\n"
                "Маленькая точка плавно двигается по видео (10% прозрачности)\n\n"
                f"**Фильтр по году:** {year_status}\n"
                "Фильтрация видео по году публикации\n\n"
                f"**Overlay изображений:** {overlays_count} шт.\n"
                f"Папка: `{Config.OVERLAYS_DIR}`\n\n"
                "Положите 2-10 изображений в папку overlays для уникализации."
            )

            return text, builder.as_markup()

        # Главное меню - Настройки
        @self.router.message(F.text == "⚙️ Настройки")
        async def menu_settings(message: Message):
            text, keyboard = _get_settings_text_and_keyboard(message.from_user.id)
            await message.answer(text, reply_markup=keyboard, parse_mode="Markdown")

        # Переключение размытого фона
        @self.router.callback_query(F.data == "toggle_blur")
        async def toggle_blur(callback: CallbackQuery):
            current = self.user_settings.get_blur_background(callback.from_user.id)
            new_value = not current
            self.user_settings.set_blur_background(callback.from_user.id, new_value)

            text, keyboard = _get_settings_text_and_keyboard(callback.from_user.id)
            await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
            await callback.answer(f"Размытый фон {'включен' if new_value else 'выключен'}")

        # Переключение эффекта снега
        @self.router.callback_query(F.data == "toggle_snow")
        async def toggle_snow(callback: CallbackQuery):
            current = self.user_settings.get_snow_effect(callback.from_user.id)
            new_value = not current
            self.user_settings.set_snow_effect(callback.from_user.id, new_value)

            text, keyboard = _get_settings_text_and_keyboard(callback.from_user.id)
            await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
            await callback.answer(f"Эффект снега {'включен' if new_value else 'выключен'}")

        # Меню фильтра по году
        @self.router.callback_query(F.data == "year_filter_menu")
        async def year_filter_menu(callback: CallbackQuery):
            year_filter = self.user_settings.get_year_filter(callback.from_user.id)
            current_mode = year_filter.get("mode", "off")
            current_year = year_filter.get("year")

            builder = InlineKeyboardBuilder()

            # Кнопки для выбора года (последние 5 лет)
            current_year_now = datetime.now().year
            years = [current_year_now - i for i in range(5)]

            for year in years:
                # Показываем статус для каждого года
                if current_mode == "only" and current_year == year:
                    prefix = "✅ Только "
                elif current_mode == "exclude" and current_year == year:
                    prefix = "🚫 Исключить "
                else:
                    prefix = ""
                builder.button(text=f"{prefix}{year}", callback_data=f"year_select:{year}")

            builder.adjust(3, 2)

            # Кнопки режимов
            builder.row(
                InlineKeyboardButton(
                    text="✅ Только выбранный" if current_mode == "only" else "Только выбранный",
                    callback_data="year_mode:only"
                ),
                InlineKeyboardButton(
                    text="🚫 Исключить выбранный" if current_mode == "exclude" else "Исключить выбранный",
                    callback_data="year_mode:exclude"
                )
            )
            builder.row(
                InlineKeyboardButton(text="❌ Сбросить фильтр", callback_data="year_mode:off")
            )
            builder.row(
                InlineKeyboardButton(text="◀️ Назад к настройкам", callback_data="back_to_settings")
            )

            mode_text = {
                "off": "выключен",
                "only": f"показывать только {current_year}",
                "exclude": f"исключить {current_year}"
            }.get(current_mode, "выключен")

            await callback.message.edit_text(
                "📅 **Фильтр по году**\n\n"
                f"Текущий режим: **{mode_text}**\n\n"
                "1. Выберите год\n"
                "2. Выберите режим:\n"
                "   • **Только выбранный** - парсить только видео этого года\n"
                "   • **Исключить выбранный** - не парсить видео этого года\n\n"
                "Это поможет находить свежий или наоборот старый контент.",
                reply_markup=builder.as_markup(),
                parse_mode="Markdown"
            )
            await callback.answer()

        # Выбор года
        @self.router.callback_query(F.data.startswith("year_select:"))
        async def year_select(callback: CallbackQuery):
            year = int(callback.data.split(":")[1])
            year_filter = self.user_settings.get_year_filter(callback.from_user.id)
            current_mode = year_filter.get("mode", "off")

            # Если режим не выбран, ставим "only" по умолчанию
            if current_mode == "off":
                current_mode = "only"

            self.user_settings.set_year_filter(callback.from_user.id, current_mode, year)
            await callback.answer(f"Выбран год: {year}")

            # Обновляем меню
            await year_filter_menu(callback)

        # Выбор режима фильтра
        @self.router.callback_query(F.data.startswith("year_mode:"))
        async def year_mode_select(callback: CallbackQuery):
            mode = callback.data.split(":")[1]
            year_filter = self.user_settings.get_year_filter(callback.from_user.id)
            current_year = year_filter.get("year")

            if mode == "off":
                self.user_settings.clear_year_filter(callback.from_user.id)
                await callback.answer("Фильтр по году отключен")
            else:
                if current_year:
                    self.user_settings.set_year_filter(callback.from_user.id, mode, current_year)
                    await callback.answer(f"Режим: {'только' if mode == 'only' else 'исключить'} {current_year}")
                else:
                    # Если год не выбран, выбираем текущий
                    current_year = datetime.now().year
                    self.user_settings.set_year_filter(callback.from_user.id, mode, current_year)
                    await callback.answer(f"Выбран год {current_year}")

            # Обновляем меню
            await year_filter_menu(callback)

        # Назад к настройкам
        @self.router.callback_query(F.data == "back_to_settings")
        async def back_to_settings(callback: CallbackQuery):
            text, keyboard = _get_settings_text_and_keyboard(callback.from_user.id)
            await callback.message.edit_text(text, reply_markup=keyboard, parse_mode="Markdown")
            await callback.answer()

        # Назад в меню
        @self.router.callback_query(F.data == "back_to_menu")
        async def back_to_menu(callback: CallbackQuery):
            await callback.message.delete()
            await callback.message.answer(
                "Выберите действие:",
                reply_markup=get_main_keyboard()
            )
            await callback.answer()

        # Добавить видео в очередь уникализации
        @self.router.callback_query(F.data.startswith("uq_add:"))
        async def add_to_unique_queue(callback: CallbackQuery):
            short_file_id = callback.data.split(":", 1)[1]
            pending_key = f"pending_video_{short_file_id}"
            video_data = self.user_settings.get(callback.from_user.id, pending_key)

            if not video_data:
                await callback.answer("❌ Видео не найдено", show_alert=True)
                return

            # Добавляем в очередь
            if self.user_settings.add_to_unique_queue(callback.from_user.id, video_data):
                queue_count = self.user_settings.get_unique_queue_count(callback.from_user.id)
                # Обновляем caption сообщения
                new_caption_parts = []
                if video_data.get("author"):
                    new_caption_parts.append(f"🎬 {video_data['author']}")
                if video_data.get("title"):
                    new_caption_parts.append(video_data["title"][:200])
                new_caption_parts.append(f"\n✅ Добавлено в очередь уникализации ({queue_count} шт)")
                new_caption = "\n".join(new_caption_parts)

                try:
                    await callback.message.edit_caption(caption=new_caption, reply_markup=None)
                except Exception:
                    pass

                await callback.answer(f"✅ Добавлено! В очереди: {queue_count}")
            else:
                await callback.answer("Видео уже в очереди")

        # Пропустить добавление в очередь
        @self.router.callback_query(F.data.startswith("uq_skip:"))
        async def skip_unique_queue(callback: CallbackQuery):
            short_file_id = callback.data.split(":", 1)[1]
            pending_key = f"pending_video_{short_file_id}"
            video_data = self.user_settings.get(callback.from_user.id, pending_key)

            # Обновляем caption
            new_caption_parts = []
            if video_data:
                if video_data.get("author"):
                    new_caption_parts.append(f"🎬 {video_data['author']}")
                if video_data.get("title"):
                    new_caption_parts.append(video_data["title"][:200])
            new_caption_parts.append("\n📥 Скачано без водяного знака")
            new_caption = "\n".join(new_caption_parts)

            try:
                await callback.message.edit_caption(caption=new_caption, reply_markup=None)
            except Exception:
                pass

            await callback.answer("Пропущено")

        # Уникализировать все из очереди
        @self.router.callback_query(F.data == "uniqueize_all")
        async def uniqueize_all(callback: CallbackQuery):
            queue = self.user_settings.get_unique_queue(callback.from_user.id)

            if not queue:
                await callback.answer("❌ Очередь пуста", show_alert=True)
                return

            await callback.message.edit_text(
                f"🚀 Запускаю уникализацию {len(queue)} видео...\n\n"
                "Видео будут обработаны и отправлены по мере готовности."
            )
            await callback.answer()

            # Добавляем все видео из очереди как задачи
            use_blur = self.user_settings.get_blur_background(callback.from_user.id)
            use_snow = self.user_settings.get_snow_effect(callback.from_user.id)
            added = 0

            for video_data in queue:
                task = VideoTask(
                    task_id="",
                    user_id=callback.from_user.id,
                    chat_id=callback.message.chat.id,
                    file_id=video_data.get("file_id"),
                    video_id=video_data.get("video_id", ""),
                    author=video_data.get("author", ""),
                    author_id=video_data.get("author_id", ""),
                    title=video_data.get("title", ""),
                    use_blur_background=use_blur,
                    use_snow_effect=use_snow,
                    do_uniqueize=True  # Режим только уникализации
                )

                if self.queue_manager.add_task(task):
                    added += 1

            # Очищаем очередь уникализации
            self.user_settings.clear_unique_queue(callback.from_user.id)

            await callback.message.answer(
                f"✅ Добавлено {added} видео в обработку!\n\n"
                "Проверить статус: 📊 Статус очереди",
                reply_markup=get_main_keyboard()
            )

        # Очистить очередь уникализации
        @self.router.callback_query(F.data == "clear_unique_queue")
        async def clear_unique_queue(callback: CallbackQuery):
            self.user_settings.clear_unique_queue(callback.from_user.id)
            await callback.message.edit_text("🗑️ Очередь уникализации очищена")
            await callback.answer("Очередь очищена")

        # Отправить видео вручную
        @self.router.callback_query(F.data == "send_video_manual")
        async def send_video_manual(callback: CallbackQuery, state: FSMContext):
            await state.set_state(BotStates.waiting_video_for_unique)
            await callback.message.edit_text(
                "🔄 **Уникализация видео**\n\n"
                "Отправьте видео или ссылку на TikTok.\n"
                "Можно отправить сразу несколько видео!\n\n"
                "Видео будет обработано и возвращено с изменениями,\n"
                "невидимыми для глаза, но уникальными для соцсетей.",
                parse_mode="Markdown"
            )
            await callback.answer()

        # Главное меню - Помощь
        @self.router.message(F.text == "❓ Помощь")
        async def menu_help(message: Message):
            overlays_count = len(self.uniqueizer._overlay_images)
            await message.answer(
                "❓ **Помощь**\n\n"
                "**🎬 Парсер TikTok**\n"
                "1. Введите запрос → выберите количество\n"
                "2. Получите видео без водяного знака\n"
                "3. Под каждым видео кнопка '✅ Уникализировать'\n"
                "4. Выберите нужные видео\n"
                "5. Перейдите в 🔄 Уникализация → Уникализировать все\n\n"
                "**🔄 Уникализация**\n"
                "Обрабатывает выбранные видео из парсера.\n"
                "Или отправьте свои видео вручную.\n\n"
                "Уникализация:\n"
                f"• Наложение 2 из {overlays_count} overlay изображений\n"
                "• Прозрачность 0.1%-1% (невидимо)\n"
                "• Случайные метаданные\n"
                "• Опционально: размытый фон\n\n"
                "**⚙️ Настройки**\n"
                "Включите размытый фон для дополнительной уникализации.\n\n"
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
            use_blur = self.user_settings.get_blur_background(message.from_user.id)
            use_snow = self.user_settings.get_snow_effect(message.from_user.id)
            task = VideoTask(
                task_id="",
                user_id=message.from_user.id,
                chat_id=message.chat.id,
                file_id=message.video.file_id,
                author="",
                title="Ваше видео",
                use_blur_background=use_blur,
                use_snow_effect=use_snow
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

            use_blur = self.user_settings.get_blur_background(message.from_user.id)
            use_snow = self.user_settings.get_snow_effect(message.from_user.id)
            task = VideoTask(
                task_id="",
                user_id=message.from_user.id,
                chat_id=message.chat.id,
                video_url=info["video_url"],
                author=info.get("author", ""),
                author_id=info.get("author_id", ""),
                title=info.get("title", ""),
                use_blur_background=use_blur,
                use_snow_effect=use_snow
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

            use_blur = self.user_settings.get_blur_background(callback.from_user.id)
            use_snow = self.user_settings.get_snow_effect(callback.from_user.id)
            task = VideoTask(
                task_id="",
                user_id=callback.from_user.id,
                chat_id=callback.message.chat.id,
                video_url=info["video_url"],
                author=info.get("author", ""),
                author_id=info.get("author_id", ""),
                title=info.get("title", ""),
                use_blur_background=use_blur,
                use_snow_effect=use_snow
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
        """Запустить парсинг видео с прогрессивным поиском"""
        try:
            # Получаем настройки фильтра года
            year_filter = self.user_settings.get_year_filter(user_id)
            year_mode = year_filter.get("mode", "off")
            filter_year = year_filter.get("year")

            # Список для сбора новых видео
            new_videos = []
            cursor = 0
            has_more = True
            total_searched = 0
            cache_skipped = 0
            year_filtered_count = 0
            max_pages = 10  # Максимум страниц поиска (защита от бесконечного цикла)
            pages_searched = 0

            await status_message.edit_text(
                f"🔍 Поиск видео по запросу: **{query}**\n"
                f"Цель: {count} новых видео...",
                parse_mode="Markdown"
            )

            # Прогрессивный поиск пока не наберём нужное количество
            while len(new_videos) < count and has_more and pages_searched < max_pages:
                pages_searched += 1

                # Получаем пакет видео
                batch_videos, cursor, has_more = await self.downloader.search_videos_batch(
                    query, count=30, cursor=cursor
                )

                if not batch_videos:
                    break

                total_searched += len(batch_videos)

                # Фильтруем уже отправленные пользователю видео
                filtered_batch = self.video_cache.filter_new_videos(batch_videos, user_id)
                batch_cache_skipped = len(batch_videos) - len(filtered_batch)
                cache_skipped += batch_cache_skipped

                # Применяем фильтр по году
                if year_mode != "off" and filter_year:
                    year_filtered_batch = []
                    for v in filtered_batch:
                        video_year = v.get("year")
                        if video_year is None:
                            year_filtered_batch.append(v)
                        elif year_mode == "only" and video_year == filter_year:
                            year_filtered_batch.append(v)
                        elif year_mode == "exclude" and video_year != filter_year:
                            year_filtered_batch.append(v)

                    year_filtered_count += len(filtered_batch) - len(year_filtered_batch)
                    filtered_batch = year_filtered_batch

                # Добавляем отфильтрованные видео (только то, что нужно)
                remaining = count - len(new_videos)
                new_videos.extend(filtered_batch[:remaining])

                # Обновляем статус поиска
                if len(new_videos) < count and has_more:
                    await status_message.edit_text(
                        f"🔍 Поиск видео по запросу: **{query}**\n"
                        f"Найдено: {len(new_videos)}/{count}\n"
                        f"Просмотрено: {total_searched} видео\n"
                        f"⏳ Продолжаем поиск...",
                        parse_mode="Markdown"
                    )
                    await asyncio.sleep(0.5)  # Небольшая задержка между запросами

            # Проверяем результаты
            if not new_videos:
                cache_stats = self.video_cache.get_stats(user_id)
                year_info = ""
                if year_mode != "off":
                    year_info = f"\n📅 Фильтр года: {'только' if year_mode == 'only' else 'исключить'} {filter_year}"

                await status_message.edit_text(
                    f"😔 По запросу **{query}** подходящих видео не найдено.\n\n"
                    f"🔍 Просмотрено: {total_searched} видео\n"
                    f"📊 В кэше: {cache_stats['user']} ваших видео{year_info}\n"
                    f"Попробуйте другой запрос или измените фильтр года в настройках.",
                    parse_mode="Markdown"
                )
                await state.clear()
                return

            # Формируем информацию о фильтрации
            skip_info_parts = []
            if cache_skipped > 0:
                skip_info_parts.append(f"{cache_skipped} уже отправленных")
            if year_filtered_count > 0:
                skip_info_parts.append(f"{year_filtered_count} по фильтру года")
            skip_info = f"(пропущено: {', '.join(skip_info_parts)})" if skip_info_parts else ""

            await status_message.edit_text(
                f"✅ Найдено **{len(new_videos)}** новых видео!\n"
                f"🔍 Просмотрено: {total_searched} видео\n"
                f"{skip_info}\n\n"
                f"Добавляю в очередь на обработку...",
                parse_mode="Markdown"
            )

            # Добавляем задачи в очередь (без уникализации - только скачивание)
            use_blur = self.user_settings.get_blur_background(user_id)
            use_snow = self.user_settings.get_snow_effect(user_id)
            added = 0
            for video in new_videos:
                task = VideoTask(
                    task_id="",
                    user_id=user_id,
                    chat_id=status_message.chat.id,
                    video_url=video["video_url"],
                    video_id=video.get("video_id", ""),
                    author=video.get("author", ""),
                    author_id=video.get("author_id", ""),
                    title=video.get("title", ""),
                    use_blur_background=use_blur,
                    use_snow_effect=use_snow,
                    skip_uniqueization=True  # Только скачивание, уникализация по выбору
                )

                if self.queue_manager.add_task(task):
                    added += 1

            cache_stats = self.video_cache.get_stats(user_id)
            final_msg = (
                f"✅ **Готово!**\n\n"
                f"🔍 Запрос: {query}\n"
                f"📦 Добавлено в очередь: {added} видео\n"
                f"🔎 Просмотрено: {total_searched} видео\n"
                f"💾 В кэше: {cache_stats['user']} ваших видео\n\n"
            )

            if added < count:
                final_msg += f"⚠️ Найдено меньше видео чем запрошено (закончились результаты поиска)\n\n"

            final_msg += (
                f"Видео будут скачаны и отправлены.\n"
                f"Под каждым видео будет кнопка для добавления в уникализацию.\n"
                f"Потом перейдите в 🔄 Уникализация и нажмите 'Уникализировать все'"
            )

            await status_message.edit_text(final_msg, parse_mode="Markdown")

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

        # Проверяем доступ к storage каналу
        try:
            chat = await self.bot.get_chat(self.storage_chat_id)
            logger.info(f"Storage channel access OK: {chat.title} (ID: {self.storage_chat_id})")
        except Exception as e:
            logger.error(f"Cannot access storage channel {self.storage_chat_id}: {e}")
            logger.error("Please check STORAGE_CHAT_ID in Config and make sure the bot is admin in that channel")
            print("=" * 50)
            print(f"ОШИБКА: Не могу получить доступ к storage каналу!")
            print(f"Storage ID: {self.storage_chat_id}")
            print(f"Ошибка: {e}")
            print()
            print("Убедитесь что:")
            print("1. STORAGE_CHAT_ID указан правильно")
            print("2. Бот добавлен в этот канал/группу как администратор")
            print("3. У бота есть права на отправку сообщений")
            print("=" * 50)
            return

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

    # Поиск FFmpeg
    ffmpeg_path = find_ffmpeg()

    if not ffmpeg_path:
        print("=" * 50)
        print("ОШИБКА: FFmpeg не найден!")
        print()
        print("Положите ffmpeg.exe в папку со скриптом:")
        print(f"  {SCRIPT_DIR}")
        print()
        print("Или установите FFmpeg в систему:")
        print("  Windows: скачайте с ffmpeg.org и распакуйте")
        print("  Ubuntu/Debian: sudo apt install ffmpeg")
        print("  macOS: brew install ffmpeg")
        print("=" * 50)
        return

    print(f"FFmpeg найден: {ffmpeg_path}")
    Config.FFMPEG_PATH = ffmpeg_path

    # Запуск бота
    bot = TikTokBot(
        token=Config.BOT_TOKEN,
        storage_chat_id=Config.STORAGE_CHAT_ID,
        ffmpeg_path=ffmpeg_path
    )

    asyncio.run(bot.start())


if __name__ == "__main__":
    main()
