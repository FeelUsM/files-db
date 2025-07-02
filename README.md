todo

добавить stat_time hash_time

walk_stat и update_hashes - запускать в отдельном потоке  
	вызывать update_hashes из walk_stat  
	update_hashes чтобы поддерживался хеш родителя  
	modified: 2 флага: для хэшей и для размеров  
		если хеш был ранее посчитан а теперь недоступен и нет событий об изменении этого файла с момента вычисления хеша - хеш не обновляем
		если хеша раньше не было и он появился - обновляем, но событие не создаём
	размер папок хранить в stat sumsize  
	переменная q - глобальная queue  
	ждать несколько секунд между вычислениями хеша  

добавить убивание потоков из watch-winreg.py

из notify убрать username - задавать как аргумент командной строки или переменная среды 

`findmnt` для определения root_dirs
поиск папок/файлов с одинаковыми dev/ino

придумать другой способ взаимодействия программ
    zmq

в интерфейсе сделать одно дерево и несколько фильтров - что отображать в этом дереве

    просмотр дерева и владельцев
        owners/* формат: 
            имя_owner-a.txt
            save/don't save
            # комментарий
            спиоск папок этого владельца
        file-desc/*
            путь_с_разделителями_%_id.txt
            описание файла - генерить нейронкой

    просмотр истории

    события, найденные статически

    поиск дубликатов

    поиск файлов

    использование диска

сделать базовый класс и два дочерних: сервер и клиент

на винде 
	владельцы
	ветки реестра - как отдельные корневые папки, и вообще попробовать в cygwin

pause, continue, stop

следить за корнем файлового дерева но не рекурсивно

для walk_stat сделать progress_bar

драйвер мониторинга реестра

events

настроить github actions чтобы запускались mypy и ruff под виндой и под линуксом

чтобы save - был набором флагов: сохранять ли и делать ли уведомления при изменении/создании/удалении/перемещении, считать ли хеши

сделать журнал с ошибками берущимися из ФС

иногда при работающем сервере на клиенте падает check_integrity - продумать транзакции

по 1-2 теста независимых на каждую функцию

кэшировать id2path/path2ids

хранить историю размеров некоторых папок

rotate(time)

sudo sysctl fs.inotify.max_user_instances=1024
sudo sysctl fs.inotify.max_queued_events=16384
sudo sysctl fs.inotify.max_user_watches=524288

cat /proc/sys/fs/inotify/max_user_instances
cat /proc/sys/fs/inotify/max_queued_events
cat /proc/sys/fs/inotify/max_user_watches

echo "fs.inotify.max_user_watches=524288" | sudo tee -a /etc/sysctl.conf
sudo sysctl -p

```bash
#=== on startup ===
cd pyfiles/src

source ../bin/activate
sudo ../bin/jupyter notebook --allow-root
jupyter notebook

source ../bin/activate
cat fifo fifo fifo fifo | sudo ../bin/python -u filesdb.py root.db | tee -a root.log 
# -u     : force the stdout and stderr streams to be unbuffered;


sqlite3 files1.db .dump > backup.sql
rm files1.db 
sqlite3 files1.db < backup.sql

mkfifo fifo
chmod 777 fifo

mypy --platform linux filesdb.py
ruff check filesdb.py
```

