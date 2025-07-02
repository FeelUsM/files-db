import sys
if sys.platform == 'win32':
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
	import subprocess
	message = 'привет' #  'при\udc88вет' - python использует surrogateescape, а glibc ругается
	if os.getuid()==0: # type: ignore[attr-defined]
		try:
			username = 'feelus'
			title = 'filesdb:'
			# Получаем DBUS_SESSION_BUS_ADDRESS
			dbus_address = subprocess.check_output(
				f"grep -z DBUS_SESSION_BUS_ADDRESS /proc/$(pgrep -u {username} plasmashell | head -1)/environ | tr '\\0' '\\n' | sed 's/DBUS_SESSION_BUS_ADDRESS=//'",
				#f"grep -z DBUS_SESSION_BUS_ADDRESS /proc/$(pgrep -u {username} gnome-session | head -n1)/environ | tr '\\0' '\\n' | sed 's/DBUS_SESSION_BUS_ADDRESS=//'",
				shell=True, text=True
			).strip()
			# Отправляем уведомление через sudo
			subprocess.run(
				["sudo", "-u", username, f"DBUS_SESSION_BUS_ADDRESS={dbus_address}", "notify-send", title, message],
				check=True
			)
		except subprocess.CalledProcessError as e:
			print(f"Ошибка при отправке уведомления: {e}")
	else:
		os.system('notify-send filesdb: "'+message+'"')
