todo

переменные класса убрать в `__items__`

в интерфейсе сделать одно дерево и несколько фильтров - что отображать в этом дереве

    просмотр дерева и владельцев
        owners/* формат: 
            имя_owner-a.txt
            save/don't save
            # комментарий
            спиоск папок этого владельца
        file-desc/*
            путь_с_разделителями_%_id.txt
            описание файла

    просмотр истории

    события, найденные статически

    поиск дубликатов

    поиск файлов

    использование диска


из notify убрать username - задавать как аргумент командной строки или переменная среды

вызывать update_hashes из walk_stat

update_hashes чтобы поддерживался хеш родителя

modified: 2 флага: для хешей и для размеров

walk_stat и update_hashes - запускать в отдельном потоке

придумать другой способ взаимодействия программ
    zmq

убрать в линуксе blocks и block_size и/или добавить в винде attrs и reparse_tag

на винде владельцы

на винде ветки реестра - как отдельные корневые папки, и вообще попробовать в cygwin

сделать базовый класс и два дочерних: сервер и клиент

размер папок хранить в stat sumsize

чтобы save - был набором флагов: сохранять ли и делать ли уведомления при изменении/создании/удалении/перемещении

иногда при работающем сервере на клиенте падает check_integrity - продумать транзакции

по 1-2 теста независимых на каждую функцию

pause, continue, stop

драйвер мониторинга реестра

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

#=== on startup ===
#cd pyfiles/src
#
#source ../bin/activate
#sudo ../bin/jupyter notebook --allow-root
#jupyter notebook
#
#source ../bin/activate
#cat fifo fifo fifo fifo | sudo ../bin/python -u filesdb.py root.db | tee -a root.log 
## -u     : force the stdout and stderr streams to be unbuffered;
#
#
#sqlite3 files1.db .dump > backup.sql
#rm files1.db 
#sqlite3 files1.db < backup.sql
#
#mkfifo fifo
#chmod 777 fifo

