from slack_bot import SlackMessage
from data_process import Engine

engine_obj = Engine()
slack_obj = SlackMessage()

engine_obj.trans_handler_ms_sql()  # данные для бот стата
slack_obj.send_message()
