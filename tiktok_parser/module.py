"""
TikTok Parser Module - готовые хэндлеры для интеграции в бота
"""

import asyncio
import re
from typing import Optional, Callable, Any

from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from .parser import TikTokParser


class TikTokStates(StatesGroup):
    """FSM состояния для парсера"""
    waiting_for_query = State()
    waiting_for_count = State()
    processing = State()


class TikTokParserModule:
    """
    Готовый модуль для интеграции в aiogram бота

    Пример использования:
        from tiktok_parser import TikTokParserModule

        # В вашем боте
        bot = Bot(token=TOKEN)
        dp = Dispatcher()

        tiktok = TikTokParserModule(
            bot=bot,
            storage_chat_id=-1001234567890  # ID вашей группы-хранилища
        )
        tiktok.register_handlers(dp)

        # Или с роутером
        router = tiktok.get_router()
        dp.include_router(router)
    """

    def __init__(
        self,
        bot,
        storage_chat_id: int,
        command: str = "tiktok",
        max_videos: int = 20,
        default_count: int = 5,
        admin_ids: Optional[list] = None,
        on_start_callback: Optional[Callable] = None,
        on_complete_callback: Optional[Callable] = None
    ):
        """
        Args:
            bot: Экземпляр aiogram.Bot
            storage_chat_id: ID группы для хранения видео (получите через @userinfobot)
            command: Команда для запуска парсера (без /)
            max_videos: Максимальное количество видео за раз
            default_count: Количество видео по умолчанию
            admin_ids: Список ID админов (None = доступ всем)
            on_start_callback: Callback при старте парсинга async def(user_id, query, count)
            on_complete_callback: Callback при завершении async def(user_id, videos_count)
        """
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

        # Команда /tiktok
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

        # Получение запроса
        @self.router.message(TikTokStates.waiting_for_query)
        async def get_query(message: Message, state: FSMContext):
            if message.text.startswith('/'):
                if message.text == '/cancel':
                    await state.clear()
                    await message.answer("❌ Отменено")
                return

            query = message.text.strip()
            if len(query) < 2:
                await message.answer("⚠️ Запрос слишком короткий. Минимум 2 символа.")
                return

            await state.update_data(query=query)
            await state.set_state(TikTokStates.waiting_for_count)

            # Клавиатура с выбором количества
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

        # Выбор количества через callback
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

            # Запускаем парсинг
            await self._process_search(callback.message, query, count, state)

        # Ввод количества текстом
        @self.router.message(TikTokStates.waiting_for_count)
        async def get_count_text(message: Message, state: FSMContext):
            if message.text.startswith('/'):
                if message.text == '/cancel':
                    await state.clear()
                    await message.answer("❌ Отменено")
                return

            try:
                count = int(message.text.strip())
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

            # Запускаем парсинг
            await self._process_search(status_msg, query, count, state)

        # Обработка TikTok ссылок
        @self.router.message(F.text.regexp(r'https?://(?:www\.)?(?:tiktok\.com|vm\.tiktok\.com)/'))
        async def handle_tiktok_link(message: Message):
            if not self._check_access(message.from_user.id):
                return

            url_match = re.search(
                r'https?://(?:www\.)?(?:tiktok\.com|vm\.tiktok\.com)/[^\s]+',
                message.text
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

    async def _process_search(
        self,
        status_message: Message,
        query: str,
        count: int,
        state: FSMContext
    ):
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
            videos = await self.parser.search_and_parse(
                query,
                count=count,
                progress_callback=progress_callback
            )

            if not videos:
                await status_message.edit_text(
                    f"😔 По запросу **{query}** видео не найдено.\n"
                    "Попробуйте другой запрос.",
                    parse_mode="Markdown"
                )
                await state.clear()
                return

            await status_message.edit_text(
                f"✅ Найдено **{len(videos)}** видео!\n"
                "Отправляю...",
                parse_mode="Markdown"
            )

            sent_count = 0
            for video in videos:
                success = await self.parser.send_video_to_user(user_id, video)
                if success:
                    sent_count += 1
                await asyncio.sleep(0.5)  # Анти-флуд

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
            await status_message.edit_text(
                f"❌ Произошла ошибка при обработке.\n"
                f"Попробуйте позже.",
                parse_mode="Markdown"
            )
        finally:
            await state.clear()

    def _check_access(self, user_id: int) -> bool:
        """Проверить доступ пользователя"""
        if self.admin_ids is None:
            return True
        return user_id in self.admin_ids

    def get_router(self) -> Router:
        """Получить роутер для включения в Dispatcher"""
        return self.router

    def register_handlers(self, dp):
        """Зарегистрировать хэндлеры в Dispatcher"""
        dp.include_router(self.router)

    async def close(self):
        """Закрыть все соединения"""
        await self.parser.close()


# Альтернативный простой интерфейс без FSM
class SimpleTikTokHandler:
    """
    Простой хэндлер без FSM для быстрой интеграции

    Использование:
        handler = SimpleTikTokHandler(bot, storage_chat_id)

        @dp.message(Command("tt"))
        async def tt_command(message: Message):
            # /tt #смешныекоты 5
            await handler.handle_command(message)
    """

    def __init__(self, bot, storage_chat_id: int):
        self.parser = TikTokParser(bot, storage_chat_id)

    async def handle_command(self, message: Message, default_count: int = 5):
        """
        Обработать команду формата: /tt <запрос> [количество]
        """
        text = message.text.split(maxsplit=2)

        if len(text) < 2:
            await message.answer(
                "Использование: /tt <запрос> [кол-во]\n"
                "Пример: /tt смешные коты 5"
            )
            return

        query = text[1] if len(text) == 2 else text[1]
        count = default_count

        # Пробуем извлечь количество
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

    async def handle_url(self, message: Message):
        """Обработать сообщение с TikTok ссылкой"""
        url_match = re.search(
            r'https?://(?:www\.)?(?:tiktok\.com|vm\.tiktok\.com)/[^\s]+',
            message.text
        )
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
