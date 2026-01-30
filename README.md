# TikTok Parser Module for Telegram Bot

Модуль для парсинга и скачивания видео с TikTok без водяного знака для Telegram ботов на aiogram 3.x.

## Возможности

- 🔍 Поиск видео по хэштегам и ключевым словам
- 📥 Скачивание видео без водяного знака
- 💾 Хранение видео в Telegram группе (не занимает место локально)
- 🚀 Кэширование file_id для быстрой повторной отправки
- 📊 Отображение статистики видео (просмотры, лайки, комментарии)
- 🔗 Скачивание по прямой ссылке на видео
- ⚙️ Легкая интеграция в существующего бота

## Установка

```bash
pip install aiogram aiohttp
```

## Быстрый старт

### 1. Создайте группу для хранения видео

1. Создайте приватную группу в Telegram
2. Добавьте своего бота с правами администратора
3. Получите ID группы через @userinfobot или @getidsbot
   - ID будет выглядеть как: `-1001234567890`

### 2. Интегрируйте в вашего бота

```python
from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage
from tiktok_parser import TikTokParserModule

# Ваш бот
bot = Bot(token="YOUR_BOT_TOKEN")
dp = Dispatcher(storage=MemoryStorage())

# Создаем модуль парсера
tiktok = TikTokParserModule(
    bot=bot,
    storage_chat_id=-1001234567890,  # ID вашей группы
    command="tiktok"                  # Команда /tiktok
)

# Регистрируем хэндлеры
tiktok.register_handlers(dp)

# Запуск
async def main():
    try:
        await dp.start_polling(bot)
    finally:
        await tiktok.close()
```

## Использование

После интеграции пользователи могут:

1. **Поиск по запросу:**
   - Ввести `/tiktok`
   - Написать поисковый запрос (например: `смешные коты` или `#funny`)
   - Выбрать количество видео
   - Получить видео без водяного знака!

2. **Скачать по ссылке:**
   - Просто отправить ссылку на TikTok видео
   - Бот автоматически скачает и отправит без водяного знака

## Настройки модуля

```python
TikTokParserModule(
    bot=bot,
    storage_chat_id=-1001234567890,
    command="tiktok",           # Команда (по умолчанию "tiktok")
    max_videos=20,              # Максимум видео за запрос
    default_count=5,            # Количество по умолчанию
    admin_ids=None,             # None = доступ всем, [123, 456] = только эти ID
    on_start_callback=...,      # Callback при старте поиска
    on_complete_callback=...    # Callback при завершении
)
```

## Примеры интеграции

### Простой хэндлер без FSM

```python
from tiktok_parser.module import SimpleTikTokHandler

handler = SimpleTikTokHandler(bot, storage_chat_id=-1001234567890)

@dp.message(Command("tt"))
async def tt_command(message: Message):
    # Формат: /tt запрос количество
    # Пример: /tt смешные коты 5
    await handler.handle_command(message)
```

### Только парсер (полный контроль)

```python
from tiktok_parser import TikTokParser

parser = TikTokParser(bot, storage_chat_id=-1001234567890)

@dp.message(Command("search"))
async def search(message: Message):
    videos = await parser.search_and_parse("котики", count=5)
    for video in videos:
        await parser.send_video_to_user(message.chat.id, video)
```

### Только скачивание по ссылке

```python
from tiktok_parser import TikTokDownloader

downloader = TikTokDownloader()

@dp.message(F.text.contains("tiktok.com"))
async def download(message: Message):
    info = await downloader.get_video_info(message.text)
    video_bytes = await downloader.download_video(info.video_url)
    await message.answer_video(video=video_bytes)
```

## Структура модуля

```
tiktok_parser/
├── __init__.py      # Экспорт классов
├── downloader.py    # Скачивание видео без водяного знака
├── parser.py        # Парсинг и кэширование
├── module.py        # Готовые хэндлеры для aiogram
└── config.py        # Шаблон конфигурации
```

## API Reference

### TikTokDownloader

```python
downloader = TikTokDownloader(timeout=30)

# Получить информацию о видео
info = await downloader.get_video_info("https://tiktok.com/...")

# Поиск видео
videos = await downloader.search_videos("котики", count=10)

# Скачать видео
video_bytes = await downloader.download_video(video_url)

# Закрыть соединения
await downloader.close()
```

### TikTokParser

```python
parser = TikTokParser(bot, storage_chat_id, max_video_duration=180)

# Поиск и загрузка в storage
videos = await parser.search_and_parse(
    query="котики",
    count=5,
    progress_callback=async_callback  # опционально
)

# Отправить пользователю
await parser.send_video_to_user(chat_id, video_data)

# Получить видео по URL
video_data = await parser.get_video_by_url("https://tiktok.com/...")
```

## Примечания

- Видео хранятся в вашей Telegram группе, не занимая место на сервере
- При повторном запросе того же видео используется кэшированный `file_id`
- Максимальная длительность видео по умолчанию: 3 минуты
- API tikwm.com бесплатный и без ограничений

## Лицензия

MIT License
