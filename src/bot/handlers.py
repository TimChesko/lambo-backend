from aiogram import Dispatcher
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, WebAppInfo
from src.database import async_session_maker
from src.services.wallet_service import create_user


async def cmd_start(message: Message):
    async with async_session_maker() as db:
        await create_user(db, message.from_user.id, message.from_user.username)
    
    keyboard = InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(
            text="🚀 Открыть веб-апп",
            web_app=WebAppInfo(url="https://dev.durak.bot/")
        )]
    ])
    
    await message.answer(
        "🔥 Мемландия началась, а значит время начинать свапать и двигать наш $LAMBO токен к вершинам. \n\n"
        "🏆 Залетай в веб апп и смотри на каком ты месте",
        reply_markup=keyboard
    )


def register_handlers(dp: Dispatcher):
    dp.message.register(cmd_start, Command("start"))

