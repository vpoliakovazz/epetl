import time

from data_process import Engine
start = time.perf_counter()

engine_obj = Engine()

engine_obj.scalium_handler()  # таблицы SC
engine_obj.trans_handler_ho()  # зависимые trans таблицы


elapsed = time.perf_counter() - start
print(f"Total script time {elapsed:0.4}")
