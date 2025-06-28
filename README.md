todo

убрать в линуксе blocks и block_size и проверить mypy

walk_stat и update_hashes - запускать в отдельном потоке
	переменные класса убрать в `__items__`
	вызывать update_hashes из walk_stat
	update_hashes чтобы поддерживался хеш родителя
	modified: 2 флага: для хэшей и для размеров
	размер папок хранить в stat sumsize
	переменная q - глобальная queue
	ждать несколько секунд между вычислениями хэша

из notify убрать username - задавать как аргумент командной строки или переменная среды

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

драйвер мониторинга реестра

events

чтобы save - был набором флагов: сохранять ли и делать ли уведомления при изменении/создании/удалении/перемещении

иногда при работающем сервере на клиенте падает check_integrity - продумать транзакции

по 1-2 теста независимых на каждую функцию

кэшировать id2path/path2ids

хранить историю размеров некоторых папок

сделать журнал с ошибками берущимися из ФС

rotate(time)

добавить description в owners

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
```

