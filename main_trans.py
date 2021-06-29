import time

from data_process import Engine
start = time.perf_counter()

engine_obj = Engine()

engine_obj.trans_handler_independent()  # независимые trans таблицы


elapsed = time.perf_counter() - start
print(f"Total script time {elapsed:0.4}")
