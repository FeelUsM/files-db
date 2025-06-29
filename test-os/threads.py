import threading
from queue import Queue
import sys
import select

q = Queue()
exit_flag = False

def keyboard_monitor():
    global exit_flag
while not exit_flag:
    # Ожидаем ввод с таймаутом 0.1 секунды
    rlist, _, _ = select.select([sys.stdin], [], [], 0.1)
    if rlist:
        line = sys.stdin.readline().strip()
        q.put(line)
        if line == 'q':
            break
        # Проверяем флаг выхода
        if exit_flag:
            break

keyboard_thr = threading.Thread(target=keyboard_monitor, daemon=True)
keyboard_thr.start()

try:
    while True:
        event = q.get()
        if event == 'q':
            break
        else:
            raise Exception(123)
        q.task_done()
except Exception:
    exit_flag = True  # Устанавливаем флаг для завершения потока
    keyboard_thr.join(timeout=0.1)  # Даем время на завершение
    raise  # Повторно вызываем исключение для корректного завершения