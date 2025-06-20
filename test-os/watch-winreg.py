import threading
import ctypes
from pprint import pprint

def terminate_thread(thread):
    if not thread.is_alive():
        return
    exc = ctypes.py_object(SystemExit)
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(
        ctypes.c_long(thread.ident), exc)
    if res == 0:
        raise ValueError("Невозможно завершить поток.")
    elif res > 1:
        ctypes.pythonapi.PyThreadState_SetAsyncExc(thread.ident, None)
        raise SystemError("Ошибка завершения потока.")

if 0:
    import win32con
    import win32api
    import pythoncom
    from win32com.client import GetObject

    def registry_event_handler():
        pythoncom.CoInitialize()
        wmi = GetObject("winmgmts:\\\\.\\root\\default")
        query = wmi.ExecNotificationQuery(
            "SELECT * FROM RegistryKeyChangeEvent "
            "WHERE Hive='HKEY_LOCAL_MACHINE' "
            "AND KeyPath='SOFTWARE\\\\Microsoft\\\\Windows\\\\CurrentVersion\\\\Run'"
        )
        while True:
            event = query.NextEvent()
            print("Изменение в реестре:", event.KeyPath)

            print('\tAddRef=');  pprint(event.AddRef)
            print('\tAssociatorsAsync_=');   pprint(event.AssociatorsAsync_)
            print('\tAssociators_=');    pprint(event.Associators_)
            print('\tClone_=');  pprint(event.Clone_)
            print('\tCompareTo_=');  pprint(event.CompareTo_)
            print('\tDeleteAsync_=');    pprint(event.DeleteAsync_)
            print('\tDelete_='); pprint(event.Delete_)
            print('\tDerivation_='); pprint(event.Derivation_)
            print('\tExecMethodAsync_=');    pprint(event.ExecMethodAsync_)
            print('\tExecMethod_='); pprint(event.ExecMethod_)
            print('\tGetIDsOfNames=');   pprint(event.GetIDsOfNames)
            print('\tGetObjectText_=');  pprint(event.GetObjectText_)
            print('\tGetText_=');    pprint(event.GetText_)
            print('\tGetTypeInfo='); pprint(event.GetTypeInfo)
            print('\tGetTypeInfoCount=');    pprint(event.GetTypeInfoCount)
            print('\tHive=');    pprint(event.Hive)
            print('\tInstancesAsync_='); pprint(event.InstancesAsync_)
            print('\tInstances_=');  pprint(event.Instances_)
            print('\tInvoke=');  pprint(event.Invoke)
            print('\tKeyPath='); pprint(event.KeyPath)
            print('\tMethods_=');    pprint(event.Methods_)
            print('\tPath_=');   pprint(event.Path_)
            print('\tProperties_='); pprint(event.Properties_)
            print('\tPutAsync_=');   pprint(event.PutAsync_)
            print('\tPut_=');    pprint(event.Put_)
            print('\tQualifiers_='); pprint(event.Qualifiers_)
            print('\tQueryInterface=');  pprint(event.QueryInterface)
            print('\tReferencesAsync_=');    pprint(event.ReferencesAsync_)
            print('\tReferences_='); pprint(event.References_)
            print('\tRefresh_=');    pprint(event.Refresh_)
            print('\tRelease='); pprint(event.Release)
            print('\tSECURITY_DESCRIPTOR='); pprint(event.SECURITY_DESCRIPTOR)
            print('\tSecurity_=');   pprint(event.Security_)
            print('\tSetFromText_=');    pprint(event.SetFromText_)
            print('\tSpawnDerivedClass_=');  pprint(event.SpawnDerivedClass_)
            print('\tSpawnInstance_=');  pprint(event.SpawnInstance_)
            print('\tSubclassesAsync_=');    pprint(event.SubclassesAsync_)
            print('\tSubclasses_='); pprint(event.Subclasses_)
            print('\tSystemProperties_=');   pprint(event.SystemProperties_)
            print('\tTIME_CREATED=');    pprint(event.TIME_CREATED)
            print('==================================================')

else:
    def registry_event_handler(hive):
        import ctypes
        import winreg
        from datetime import datetime

        # Константы для доступа и фильтра
        KEY_NOTIFY = 0x0010
        REG_NOTIFY_CHANGE_NAME = 0x00000001
        REG_NOTIFY_CHANGE_ATTRIBUTES = 0x00000002
        REG_NOTIFY_CHANGE_LAST_SET = 0x00000004
        REG_NOTIFY_CHANGE_SECURITY = 0x00000008

        # Подключаем нужную библиотеку
        advapi32 = ctypes.WinDLL("Advapi32.dll")

        # Открываем ключ (например, автозагрузка пользователя)
        key_path = r""
        key = winreg.OpenKey(hive, key_path, 0, winreg.KEY_READ | KEY_NOTIFY)

        # Создаем событие
        event = ctypes.windll.kernel32.CreateEventW(None, True, False, None)

        # Цикл ожидания изменений
        #print(f"Ожидаем изменения ключа: HKEY_CURRENT_USER\\{key_path}")
        while True:
            rc = advapi32.RegNotifyChangeKeyValue(
                key.handle,
                True,  # Следим за подключами
                REG_NOTIFY_CHANGE_NAME | REG_NOTIFY_CHANGE_LAST_SET,
                event,
                True   # Асинхронный режим
            )

            # Ждем сигнала об изменении
            ctypes.windll.kernel32.WaitForSingleObject(event, 0xFFFFFFFF)
            print("Обнаружено изменение в реестре!",datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

            # Сброс события
            ctypes.windll.kernel32.ResetEvent(event)

        # https://stackoverflow.com/questions/79673752/how-to-determine-which-key-in-windows-registry-has-been-changed/79673838#79673838
        # https://chatgpt.com/share/6855e162-9944-800a-b448-2dbf5ca6f51b

import winreg
threads = []
for hive in [winreg.HKEY_CLASSES_ROOT,winreg.HKEY_CURRENT_USER,winreg.HKEY_LOCAL_MACHINE,winreg.HKEY_USERS,winreg.HKEY_CURRENT_CONFIG]:
    thr = threading.Thread(target = registry_event_handler, args=(hive,), name='registry_event_handler', daemon=True)
    thr.start()
    threads.append(thr)

#f = open('reg.log','a')
#print('start',file=f)
print('start, to stop type q')
x= ''
while x!='q':
    try:
        x = input()
    except EOFError:
        x = 'q'
    #print('>>',repr(x),file=f)
    #print('>>',repr(x))
#print('stop',file=f)
print('stop')
for thr in threads:
    terminate_thread(thr)  # Опасный метод!
        
