import platform
if platform.system() == 'Windows':
    from time import sleep
    if 0:
        from win10toast import ToastNotifier

        toast = ToastNotifier()
        toast.show_toast(
            "Заголовок уведомления",
            "Текст уведомления",
            duration=10,  # время показа в секундах
            icon_path=None,  # можно указать путь к иконке (.ico)
            threaded=True  # если True, программа продолжит работу, не дожидаясь закрытия уведомления
        )

    else:
        from plyer import notification

        notification.notify(
            title="Заголовок уведомления",
            message="Текст уведомления",
            app_name="Python",  # название приложения (опционально)
            timeout=10,  # время показа в секундах
            app_icon=None  # путь к иконке (.ico, опционально)
        )        

    for i in range(12,0,-1):
        print(i)
        sleep(1)
else:
    import os
    import ctypes
    import struct

    # Константы fanotify
    FAN_CLASS_NOTIF = 0x00000000
    FAN_NONBLOCK = 0x00000002
    FAN_MARK_ADD = 0x00000001
    FAN_EVENT_ACCESS = 0x00000001
    FAN_EVENT_MODIFY = 0x00000002
    FAN_EVENT_OPEN = 0x00000020

    # Загрузка libc
    libc = ctypes.CDLL('libc.so.6')

    # Инициализация fanotify
    fd = libc.fanotify_init(FAN_CLASS_NOTIF | FAN_NONBLOCK, os.O_RDONLY)
    if fd < 0:
        raise OSError("Не удалось инициализировать fanotify")

    # Добавляем мониторинг директории /boot
    ret = libc.fanotify_mark(fd, FAN_MARK_ADD, FAN_EVENT_MODIFY, -1, b'/home')
    if ret < 0:
        raise OSError("Не удалось добавить метку fanotify")

    print("Мониторинг запущен для /boot и всех подпапок...")

    # Буфер для чтения событий
    event_size = struct.calcsize("iII")
    buffer_size = 4096

    # Цикл чтения событий
    while True:
        buffer = os.read(fd, buffer_size)
        if not buffer:
            continue

        # Обрабатываем каждый event в буфере
        for i in range(0, len(buffer), event_size):
            fan_event = buffer[i:i + event_size]
            fd, mask, pid = struct.unpack("iII", fan_event)

            # Выводим информацию о событии
            print(f"Файл изменён! FD: {fd}, PID: {pid}, Событие: {mask}")
