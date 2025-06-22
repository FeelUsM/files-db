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

    def registry_event_handler(x):
        pythoncom.CoInitialize()
        wmi = GetObject("winmgmts:\\\\.\\root\\default")
        query = wmi.ExecNotificationQuery(
            "SELECT * FROM RegistryKeyChangeEvent "
            "WHERE Hive='HKEY_LOCAL_MACHINE' "
            #"AND KeyPath='SOFTWARE\\\\Microsoft\\\\Windows\\\\CurrentVersion\\\\Run'"
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

elif 0:
    def registry_event_handler(name,hive):
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

            print(type(event), event)
            # Ждем сигнала об изменении
            ctypes.windll.kernel32.WaitForSingleObject(event, 0xFFFFFFFF)
            print("Обнаружено изменение в реестре!",datetime.now().strftime("%Y-%m-%d %H:%M:%S"), name)

            # Сброс события
            ctypes.windll.kernel32.ResetEvent(event)

        # https://stackoverflow.com/questions/79673752/how-to-determine-which-key-in-windows-registry-has-been-changed/79673838#79673838
        # https://chatgpt.com/share/6855e162-9944-800a-b448-2dbf5ca6f51b

else:
    def registry_event_handler(name,hive):
        #import pythoncom
        from win32com.client import GetObject
        from pprint import pprint

        print(f"Ожидание изменений в {name}... (нажмите Ctrl+C для выхода)")

        # Получаем доступ к WMI (namespace root\default для Registry событий)
        wmi = GetObject("winmgmts:\\\\.\\root\\default")

        # Подписываемся на RegistryTreeChangeEvent в HKLM (вся ветка)
        query = f"""
        SELECT * FROM RegistryTreeChangeEvent
        WHERE Hive = '{name}' AND RootPath = ''
        """

        # Выполняем подписку
        watcher = wmi.ExecNotificationQuery(query)

        # Основной цикл ожидания событий
        while True:
            event = watcher.NextEvent()  # Блокирующий вызов, ждет события
            print("Изменение в HKEY_LOCAL_MACHINE или поддереве!")#, dir(event))
            #print('\tAddRef =', str(event.AddRef))   # AddRef = <bound method AddRef of <COMObject NextEvent>>
            #print('\tAssociatorsAsync_ =', str(event.AssociatorsAsync_))   # AssociatorsAsync_ = <bound method AssociatorsAsync_ of <COMObject NextEvent>>
            #print('\tAssociators_ =', str(event.Associators_))   # Associators_ = <bound method Associators_ of <COMObject NextEvent>>
            #print('\tClone_ =', str(event.Clone_))   # Clone_ = <bound method Clone_ of <COMObject NextEvent>>
            #print('\tCompareTo_ =', str(event.CompareTo_))   # CompareTo_ = <bound method CompareTo_ of <COMObject NextEvent>>
            #print('\tDeleteAsync_ =', str(event.DeleteAsync_))   # DeleteAsync_ = <bound method DeleteAsync_ of <COMObject NextEvent>>
            #print('\tDelete_ =', str(event.Delete_))   # Delete_ = <bound method Delete_ of <COMObject NextEvent>>
            print('\tDerivation_ =', str(event.Derivation_))   # Derivation_ = ('RegistryEvent', '__ExtrinsicEvent', '__Event', '__IndicationRelated', '__SystemClass')
            #print('\tExecMethodAsync_ =', str(event.ExecMethodAsync_))   # ExecMethodAsync_ = <bound method ExecMethodAsync_ of <COMObject NextEvent>>
            #print('\tExecMethod_ =', str(event.ExecMethod_))   # ExecMethod_ = <bound method ExecMethod_ of <COMObject NextEvent>>
            #print('\tGetIDsOfNames =', str(event.GetIDsOfNames))   # GetIDsOfNames = <bound method GetIDsOfNames of <COMObject NextEvent>>
            #print('\tGetObjectText_ =', str(event.GetObjectText_))   # GetObjectText_ = <bound method GetObjectText_ of <COMObject NextEvent>>
            #print('\tGetText_ =', str(event.GetText_))   # GetText_ = <bound method GetText_ of <COMObject NextEvent>>
            #print('\tGetTypeInfo =', str(event.GetTypeInfo))   # GetTypeInfo = <bound method GetTypeInfo of <COMObject NextEvent>>
            #print('\tGetTypeInfoCount =', str(event.GetTypeInfoCount))   # GetTypeInfoCount = <bound method GetTypeInfoCount of <COMObject NextEvent>>
            print('\tHive =', str(event.Hive))   # Hive = HKEY_LOCAL_MACHINE
            #print('\tInstancesAsync_ =', str(event.InstancesAsync_))   # InstancesAsync_ = <bound method InstancesAsync_ of <COMObject NextEvent>>
            #print('\tInstances_ =', str(event.Instances_))   # Instances_ = <bound method Instances_ of <COMObject NextEvent>>
            #print('\tInvoke =', str(event.Invoke))   # Invoke = <bound method Invoke of <COMObject NextEvent>>
            print('\tMethods_ =', str(event.Methods_))   # Methods_ = <COMObject <unknown>>
            print('\tPath_ =', str(event.Path_), dir(event.Path_))   # Path_ =

            print('Authority = ',event.Path_.Authority)
            print('Class = ',event.Path_.Class)
            print('DisplayName = ',event.Path_.DisplayName)
            print('IsClass = ',event.Path_.IsClass)
            print('IsSingleton = ',event.Path_.IsSingleton)
            print('Keys = ',event.Path_.Keys)
            print('Locale = ',event.Path_.Locale)
            print('Namespace = ',event.Path_.Namespace)
            print('ParentNamespace = ',event.Path_.ParentNamespace)
            print('Path = ',event.Path_.Path)
            print('RelPath = ',event.Path_.RelPath)
            print('Security_ = ',event.Path_.Security_)
            print('Server = ',event.Path_.Server)
            print('SetAsClass = ',event.Path_.SetAsClass)
            print('SetAsSingleton = ',event.Path_.SetAsSingleton)



            
            print('\tProperties_ =', str(event.Properties_))   # Properties_ = <COMObject <unknown>>
            #print('\tPutAsync_ =', str(event.PutAsync_))   # PutAsync_ = <bound method PutAsync_ of <COMObject NextEvent>>
            #print('\tPut_ =', str(event.Put_))   # Put_ = <bound method Put_ of <COMObject NextEvent>>
            print('\tQualifiers_ =', str(event.Qualifiers_))   # Qualifiers_ = <COMObject <unknown>>
            #print('\tQueryInterface =', str(event.QueryInterface))   # QueryInterface = <bound method QueryInterface of <COMObject NextEvent>>
            #print('\tReferencesAsync_ =', str(event.ReferencesAsync_))   # ReferencesAsync_ = <bound method ReferencesAsync_ of <COMObject NextEvent>>
            #print('\tReferences_ =', str(event.References_))   # References_ = <bound method References_ of <COMObject NextEvent>>
            #print('\tRefresh_ =', str(event.Refresh_))   # Refresh_ = <bound method Refresh_ of <COMObject NextEvent>>
            #print('\tRelease =', str(event.Release))   # Release = <bound method Release of <COMObject NextEvent>>
            print('\tRootPath =', str(event.RootPath))   # RootPath =
            print('\tSECURITY_DESCRIPTOR =', str(event.SECURITY_DESCRIPTOR))   # SECURITY_DESCRIPTOR = None
            print('\tSecurity_ =', str(event.Security_))   # Security_ = <COMObject <unknown>>
            #print('\tSetFromText_ =', str(event.SetFromText_))   # SetFromText_ = <bound method SetFromText_ of <COMObject NextEvent>>
            #print('\tSpawnDerivedClass_ =', str(event.SpawnDerivedClass_))   # SpawnDerivedClass_ = <bound method SpawnDerivedClass_ of <COMObject NextEvent>>
            #print('\tSpawnInstance_ =', str(event.SpawnInstance_))   # SpawnInstance_ = <bound method SpawnInstance_ of <COMObject NextEvent>>
            #print('\tSubclassesAsync_ =', str(event.SubclassesAsync_))   # SubclassesAsync_ = <bound method SubclassesAsync_ of <COMObject NextEvent>>
            #print('\tSubclasses_ =', str(event.Subclasses_))   # Subclasses_ = <bound method Subclasses_ of <COMObject NextEvent>>
            print('\tSystemProperties_ =', str(event.SystemProperties_))   # SystemProperties_ = <COMObject <unknown>>
            print('\tTIME_CREATED =', str(event.TIME_CREATED))   # TIME_CREATED = 133951042200066923


import winreg
threads = []
for name,hive in [
    #('HKEY_CLASSES_ROOT',winreg.HKEY_CLASSES_ROOT),
    #('HKEY_CURRENT_USER',winreg.HKEY_CURRENT_USER),
    ('HKEY_LOCAL_MACHINE',winreg.HKEY_LOCAL_MACHINE),
    #('HKEY_USERS',winreg.HKEY_USERS),
    #('HKEY_CURRENT_CONFIG',winreg.HKEY_CURRENT_CONFIG)
    ]:
    #input('create thread')
    thr = threading.Thread(target = registry_event_handler, args=(name,hive), name='registry_event_handler', daemon=True)
    #input('start thread')
    thr.start()
    threads.append(thr)

#f = open('reg.log','w')
#print('start',file=f)
print('start, to stop type q')
#input(1)
#input(2)
#input(3)
#input(4)
x= ''
while x!='q':
    try:
        x = input()
    except EOFError:
        x = 'q'
    #print('>>',repr(x),file=f)
    print('>>',repr(x))
#print('stop',file=f)
print('stop')
for thr in threads:
    terminate_thread(thr)  # Опасный метод!
        
