"""
TikTok Parser - основная логика парсинга и кэширования
"""

import asyncio
from typing import Optional, Dict, Any, List
from dataclasses import dataclass, field
from datetime import datetime
from io import BytesIO

from .downloader import TikTokDownloader


@dataclass
class CachedVideo:
    """Кэшированное видео в Telegram"""
    file_id: str  # Telegram file_id для повторной отправки
    video_url: str
    author: str
    title: str
    cached_at: datetime = field(default_factory=datetime.now)
    play_count: int = 0
    like_count: int = 0


class TikTokParser:
    """
    Парсер TikTok видео с кэшированием в Telegram группе

    Видео скачиваются без водяного знака и отправляются в storage группу,
    затем file_id сохраняется в кэш для быстрой повторной отправки
    """

    def __init__(
        self,
        bot,  # aiogram Bot instance
        storage_chat_id: int,
        max_video_duration: int = 180,  # макс длительность в секундах
        max_cache_size: int = 1000
    ):
        """
        Args:
            bot: Экземпляр aiogram.Bot
            storage_chat_id: ID группы/канала для хранения видео
            max_video_duration: Максимальная длительность видео (сек)
            max_cache_size: Максимальный размер кэша
        """
        self.bot = bot
        self.storage_chat_id = storage_chat_id
        self.max_video_duration = max_video_duration
        self.max_cache_size = max_cache_size

        self.downloader = TikTokDownloader()
        self._cache: Dict[str, CachedVideo] = {}  # video_id -> CachedVideo

    async def close(self):
        """Закрыть все соединения"""
        await self.downloader.close()

    def _add_to_cache(self, video_id: str, cached: CachedVideo):
        """Добавить видео в кэш с проверкой размера"""
        if len(self._cache) >= self.max_cache_size:
            # Удаляем самое старое
            oldest_key = min(self._cache.keys(), key=lambda k: self._cache[k].cached_at)
            del self._cache[oldest_key]

        self._cache[video_id] = cached

    async def search_and_parse(
        self,
        query: str,
        count: int = 5,
        progress_callback=None
    ) -> List[Dict[str, Any]]:
        """
        Найти и подготовить видео по запросу

        Args:
            query: Поисковый запрос (хэштег или ключевые слова)
            count: Количество видео
            progress_callback: Async callback(current, total, video_info) для прогресса

        Returns:
            Список словарей с информацией о видео и file_id
        """
        # Поиск видео
        videos = await self.downloader.search_videos(query, count=count)

        if not videos:
            return []

        results = []

        for i, video in enumerate(videos):
            video_id = video.get("video_id", "")

            # Проверяем длительность
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

                # Отправляем в storage группу
                caption = f"🎬 @{video.get('author_id', 'unknown')}\n{video.get('title', '')[:200]}"

                msg = await self.bot.send_video(
                    chat_id=self.storage_chat_id,
                    video=video_io,
                    caption=caption,
                    disable_notification=True
                )

                file_id = msg.video.file_id

                # Сохраняем в кэш
                cached = CachedVideo(
                    file_id=file_id,
                    video_url=video_url,
                    author=video.get("author", ""),
                    title=video.get("title", ""),
                    play_count=video.get("play_count", 0),
                    like_count=video.get("like_count", 0)
                )
                self._add_to_cache(video_id, cached)

                results.append({
                    "file_id": file_id,
                    "from_cache": False,
                    **video
                })

                if progress_callback:
                    await progress_callback(i + 1, len(videos), video)

                # Задержка между загрузками
                await asyncio.sleep(1)

            except Exception as e:
                print(f"Error processing video {video_id}: {e}")
                continue

        return results

    async def send_video_to_user(
        self,
        chat_id: int,
        video_data: Dict[str, Any],
        show_stats: bool = True
    ) -> bool:
        """
        Отправить видео пользователю

        Args:
            chat_id: ID чата пользователя
            video_data: Словарь с данными видео (должен содержать file_id)
            show_stats: Показывать ли статистику

        Returns:
            True если успешно
        """
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

            await self.bot.send_video(
                chat_id=chat_id,
                video=file_id,
                caption=caption,
                parse_mode="Markdown"
            )
            return True

        except Exception as e:
            print(f"Error sending video to user: {e}")
            return False

    @staticmethod
    def _format_number(num: int) -> str:
        """Форматировать число (1000 -> 1K, 1000000 -> 1M)"""
        if num >= 1_000_000:
            return f"{num / 1_000_000:.1f}M"
        elif num >= 1_000:
            return f"{num / 1_000:.1f}K"
        return str(num)

    async def get_video_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """
        Получить видео по прямой TikTok ссылке

        Args:
            url: URL TikTok видео

        Returns:
            Словарь с данными видео и file_id
        """
        info = await self.downloader.get_video_info(url)
        if not info:
            return None

        # Скачиваем и загружаем
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
