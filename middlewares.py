# Задел для будущих middlewares (логирование, ограничение скорости и т.д.)
# Можно реализовать:
# from aiogram import BaseMiddleware
# class LoggingMiddleware(BaseMiddleware):
#     async def __call__(self, handler, event, data):
#         print("Event:", type(event))
#         return await handler(event, data)