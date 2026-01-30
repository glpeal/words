"""
TikTok Video Downloader - скачивание видео без водяного знака
"""

import aiohttp
import asyncio
from typing import Optional, Dict, Any
from dataclasses import dataclass
from io import BytesIO


@dataclass
class VideoInfo:
    """Информация о видео"""
    video_url: str  # URL видео без водяного знака
    video_url_watermark: str  # URL с водяным знаком (backup)
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


class TikTokDownloader:
    """
    Скачивает видео с TikTok без водяного знака
    Использует tikwm.com API (бесплатный, без лимитов)
    """

    TIKWM_API = "https://www.tikwm.com/api/"
    TIKWM_FEED_API = "https://www.tikwm.com/api/feed/search"

    def __init__(self, timeout: int = 30):
        self.timeout = aiohttp.ClientTimeout(total=timeout)
        self._session: Optional[aiohttp.ClientSession] = None

    async def _get_session(self) -> aiohttp.ClientSession:
        """Получить или создать aiohttp сессию"""
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=self.timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
                }
            )
        return self._session

    async def close(self):
        """Закрыть сессию"""
        if self._session and not self._session.closed:
            await self._session.close()

    async def get_video_info(self, url: str) -> Optional[VideoInfo]:
        """
        Получить информацию о видео по URL

        Args:
            url: Ссылка на TikTok видео

        Returns:
            VideoInfo или None если не удалось получить
        """
        session = await self._get_session()

        try:
            async with session.post(
                self.TIKWM_API,
                data={"url": url, "hd": 1}
            ) as response:
                if response.status != 200:
                    return None

                data = await response.json()

                if data.get("code") != 0:
                    return None

                video_data = data.get("data", {})

                return VideoInfo(
                    video_url=video_data.get("play", ""),
                    video_url_watermark=video_data.get("wmplay", ""),
                    author=video_data.get("author", {}).get("nickname", "Unknown"),
                    author_id=video_data.get("author", {}).get("unique_id", ""),
                    title=video_data.get("title", ""),
                    cover_url=video_data.get("cover", ""),
                    music_title=video_data.get("music_info", {}).get("title", ""),
                    play_count=video_data.get("play_count", 0),
                    like_count=video_data.get("digg_count", 0),
                    comment_count=video_data.get("comment_count", 0),
                    share_count=video_data.get("share_count", 0),
                    duration=video_data.get("duration", 0),
                    original_url=url
                )
        except Exception as e:
            print(f"Error getting video info: {e}")
            return None

    async def search_videos(
        self,
        query: str,
        count: int = 10,
        cursor: int = 0
    ) -> list[Dict[str, Any]]:
        """
        Поиск видео по ключевым словам/хэштегам

        Args:
            query: Поисковый запрос (хэштег или ключевые слова)
            count: Количество видео (макс 30 за запрос)
            cursor: Смещение для пагинации

        Returns:
            Список словарей с информацией о видео
        """
        session = await self._get_session()

        # Убираем # если есть
        query = query.lstrip('#')

        videos = []
        remaining = count

        while remaining > 0:
            batch_count = min(remaining, 30)

            try:
                async with session.post(
                    self.TIKWM_FEED_API,
                    data={
                        "keywords": query,
                        "count": batch_count,
                        "cursor": cursor,
                        "hd": 1
                    }
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

                    # Небольшая задержка между запросами
                    await asyncio.sleep(0.5)

            except Exception as e:
                print(f"Error searching videos: {e}")
                break

        return videos[:count]

    async def download_video(self, video_url: str) -> Optional[BytesIO]:
        """
        Скачать видео в память (BytesIO)

        Args:
            video_url: URL видео (без водяного знака)

        Returns:
            BytesIO объект с видео или None
        """
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

    async def download_video_by_url(self, tiktok_url: str) -> Optional[BytesIO]:
        """
        Скачать видео по TikTok URL (комбинированный метод)

        Args:
            tiktok_url: Ссылка на TikTok видео

        Returns:
            BytesIO объект с видео или None
        """
        info = await self.get_video_info(tiktok_url)
        if not info:
            return None

        return await self.download_video(info.video_url)
