import ctypes
from ctypes import wintypes

advapi32 = ctypes.WinDLL('advapi32', use_last_error=True)
kernel32 = ctypes.WinDLL('kernel32', use_last_error=True)

# Типы
LPSECURITY_DESCRIPTOR = wintypes.LPVOID
PSID = wintypes.LPVOID

# Константы
OWNER_SECURITY_INFORMATION = 0x00000001
ERROR_INSUFFICIENT_BUFFER = 122

# Объявляем API
GetFileSecurityW = advapi32.GetFileSecurityW
GetFileSecurityW.argtypes = [wintypes.LPCWSTR, wintypes.DWORD, LPSECURITY_DESCRIPTOR, wintypes.DWORD, ctypes.POINTER(wintypes.DWORD)]
GetFileSecurityW.restype = wintypes.BOOL

GetSecurityDescriptorOwner = advapi32.GetSecurityDescriptorOwner
GetSecurityDescriptorOwner.argtypes = [LPSECURITY_DESCRIPTOR, ctypes.POINTER(PSID), ctypes.POINTER(wintypes.BOOL)]
GetSecurityDescriptorOwner.restype = wintypes.BOOL

ConvertSidToStringSidW = advapi32.ConvertSidToStringSidW
ConvertSidToStringSidW.argtypes = [PSID, ctypes.POINTER(wintypes.LPWSTR)]
ConvertSidToStringSidW.restype = wintypes.BOOL

LookupAccountSidW = advapi32.LookupAccountSidW
LookupAccountSidW.argtypes = [
    wintypes.LPCWSTR,  # lpSystemName
    PSID,              # Sid
    wintypes.LPWSTR,   # Name
    ctypes.POINTER(wintypes.DWORD),
    wintypes.LPWSTR,
    ctypes.POINTER(wintypes.DWORD),
    ctypes.POINTER(wintypes.DWORD),
]
LookupAccountSidW.restype = wintypes.BOOL

ConvertStringSidToSidW = advapi32.ConvertStringSidToSidW
ConvertStringSidToSidW.argtypes = [wintypes.LPCWSTR, ctypes.POINTER(PSID)]
ConvertStringSidToSidW.restype = wintypes.BOOL


def get_file_owner_sid(path: str) -> str:
    needed = wintypes.DWORD(0)
    GetFileSecurityW(path, OWNER_SECURITY_INFORMATION, None, 0, ctypes.byref(needed))
    if ctypes.get_last_error() != ERROR_INSUFFICIENT_BUFFER:
        raise ctypes.WinError(ctypes.get_last_error())

    sd_buf = ctypes.create_string_buffer(needed.value)
    if not GetFileSecurityW(path, OWNER_SECURITY_INFORMATION, sd_buf, needed, ctypes.byref(needed)):
        raise ctypes.WinError(ctypes.get_last_error())

    owner_sid = PSID()
    defaulted = wintypes.BOOL()
    if not GetSecurityDescriptorOwner(sd_buf, ctypes.byref(owner_sid), ctypes.byref(defaulted)):
        raise ctypes.WinError(ctypes.get_last_error())

    sid_str_ptr = wintypes.LPWSTR()
    if not ConvertSidToStringSidW(owner_sid, ctypes.byref(sid_str_ptr)):
        raise ctypes.WinError(ctypes.get_last_error())

    sid_str = sid_str_ptr.value
    kernel32.LocalFree(sid_str_ptr)
    return sid_str


def lookup_account_name_from_sid(sid_str: str) -> str:
    sid = PSID()
    if not ConvertStringSidToSidW(sid_str, ctypes.byref(sid)):
        raise ctypes.WinError(ctypes.get_last_error())

    name_len = wintypes.DWORD(0)
    domain_len = wintypes.DWORD(0)
    sid_type = wintypes.DWORD()

    LookupAccountSidW(None, sid, None, ctypes.byref(name_len), None, ctypes.byref(domain_len), ctypes.byref(sid_type))
    if ctypes.get_last_error() != ERROR_INSUFFICIENT_BUFFER:
        raise ctypes.WinError(ctypes.get_last_error())

    name_buf = ctypes.create_unicode_buffer(name_len.value)
    domain_buf = ctypes.create_unicode_buffer(domain_len.value)

    if not LookupAccountSidW(None, sid, name_buf, ctypes.byref(name_len), domain_buf, ctypes.byref(domain_len), ctypes.byref(sid_type)):
        raise ctypes.WinError(ctypes.get_last_error())

    return f"{domain_buf.value}\\{name_buf.value}"

sid = get_file_owner_sid('.')
print(sid)
print(lookup_account_name_from_sid(sid))
