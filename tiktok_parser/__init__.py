"""
TikTok Parser Module for Telegram Bot
Модуль для парсинга и скачивания видео с TikTok без водяного знака

Usage:
    from tiktok_parser import TikTokParserModule

    # Initialize in your bot
    tiktok_module = TikTokParserModule(
        bot=your_bot,
        storage_chat_id=YOUR_STORAGE_CHAT_ID  # ID группы для хранения видео
    )

    # Register handlers
    tiktok_module.register_handlers(dp)  # для aiogram
"""

from .parser import TikTokParser
from .module import TikTokParserModule
from .downloader import TikTokDownloader

__all__ = ['TikTokParser', 'TikTokParserModule', 'TikTokDownloader']
__version__ = '1.0.0'
