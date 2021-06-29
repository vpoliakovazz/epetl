import time

from data_process import Engine
start = time.perf_counter()

engine_obj = Engine()

engine_obj.ga_handler()  # таблицы GA

elapsed = time.perf_counter() - start
print(f"Total script time {elapsed:0.4}")
