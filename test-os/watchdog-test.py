import time
from datetime import datetime

from watchdog.events import FileSystemEvent, FileSystemEventHandler
from watchdog.observers import Observer

class MyEventHandler(FileSystemEventHandler):
    def on_any_event(self, event: FileSystemEvent) -> None:
        if not event.src_path.startswith('/home/feelus/snap/firefox/') and \
            not event.src_path.startswith('/home/feelus/snap/telegram-desktop/') and \
            not event.src_path.startswith('/home/feelus/.config/sublime-text') and\
            not event.src_path.startswith('/home/feelus/snap/sqlitebrowser') and\
            not event.event_type=='closed_no_write' and not event.event_type=='opened':
            print(datetime.fromtimestamp(time.time()), event.event_type[:7], 'd' if event.is_directory else 'f', event.src_path, event.dest_path if event.event_type=='moved' else '', '!!!!!' if event.is_synthetic else '', sep='\t')

def start(path):
    event_handler = MyEventHandler()
    observer = Observer()
    observer.schedule(event_handler, path, recursive=True)
    observer.start()
    print('start ibserving',path)
    return observer

print(Observer.__name__)
observers = []

import platform
if platform.system() == 'Windows':
    observers.append(start('C:\\'))
elif platform.system().startswith('CYGWIN'):
    observers.append(start('/cygdrive/c'))
else:
    observers.append(start('/boot'))
    observers.append(start('/root'))
    observers.append(start('/home'))

    observers.append(start('/snap'))

    observers.append(start('/bin.usr-is-merged'))
    observers.append(start('/sbin.usr-is-merged'))
    observers.append(start('/lib.usr-is-merged'))
    observers.append(start('/usr'))
    observers.append(start('/etc'))

    observers.append(start('/opt'))
    observers.append(start('/srv'))

    observers.append(start('/var'))
    observers.append(start('/tmp'))

    #start('/lost+found')

    #start('/media')
    #start('/cdrom')
    #start('/mnt')
    #
    #start('/proc')
    #start('/sys')
    #start('/dev')
    #start('/run')

    ['media','cdrom','mnt','proc','sys','dev','run']

print('start watch')
try:
    while True:
        print(input())
finally:
    for observer in observers:
        observer.stop()
        observer.join()