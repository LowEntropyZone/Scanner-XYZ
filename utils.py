from datetime import datetime, timedelta

import time

def proxy_config(proxy):
    return {
        'http': proxy,
        'https': proxy
    }

def date_secs(dt):
    return (dt.minute*60)+dt.second

def cur_secs():
    return date_secs(datetime.utcnow())

def start_of_hr():
    return datetime.utcnow() - timedelta(seconds=cur_secs())

import base64
def b64_e(t):
    return base64.b64encode(str(t))

def b64_d(t):
    return base64.b64decode(t)

def now_ms():
    return int(round(time.time() * 1000))

def get_iv(pokemon):
    b = (pokemon['individual_attack']+pokemon['individual_defense']+pokemon['individual_stamina']) / 45
    return b * 100

def dt_to_ts(dt):
    return int(time.mktime(dt.timetuple()) * 1000)