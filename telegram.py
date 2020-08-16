import telebot
from airflow.models.baseoperator import BaseOperator
from airflow.utils.decorators import apply_defaults


class TelegramOperator(BaseOperator):
    @apply_defaults
    def __init__(self, bot_token, send_to, msg, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.bot_token = bot_token
        self.send_to = send_to
        self.msg = msg

    def execute(self, context):
        tb = telebot.TeleBot(token=self.bot_token)
        tb.send_message(self.send_to, self.msg)
        return f'send to:{self.send_to}, msg: {self.msg}'
