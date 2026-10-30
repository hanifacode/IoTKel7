# edge1_phone.py - Edge 1 (HP Android via Termux)
# Baca GPS + accelerometer -> olah RMS -> publish telemetry ke MQTT
import argparse, json, math, random, subprocess, threading, time
from collections import deque
import paho.mqtt.client as mqtt

BROKER, PORT = 'broker.hivemq.com', 1883
UID = 'kelompok_07'                      # GANTI, harus sama di semua file
T_TELE = f'otwtracker/{UID}/telemetry'
T_STATE = f'otwtracker/{UID}/edge1/state'
WINDOW_S, G = 2.0, 9.81
HOME = (-6.2000, 106.8167)                 # hanya dipakai simulasi

accel_buf = []                             # sampel |a| pada jendela berjalan
last_gps = {'lat': None, 'lon': None, 'acc_m': None, 'speed_mps': None, 'ts': 0}
lock = threading.Lock()


# ---------- pembaca sensor asli (Termux:API) ----------
def accel_reader_real():
    # termux-sensor mengalirkan JSON multi-baris; gabungkan sampai kurung kurawal seimbang
    p = subprocess.Popen(['termux-sensor', '-s', 'accelerometer', '-d', '50'],
                         stdout=subprocess.PIPE, text=True)
    buf, depth = '', 0
    for line in p.stdout:
        buf += line
        depth += line.count('{') - line.count('}')
        if depth == 0 and buf.strip():
            try:
                obj = json.loads(buf)
                x, y, z = next(iter(obj.values()))['values']
                with lock:
                    accel_buf.append(math.sqrt(x * x + y * y + z * z))
            except (ValueError, KeyError, StopIteration):
                pass
            buf = ''


def gps_reader_real():
    while True:
        try:
            out = subprocess.run(['termux-location', '-p', 'gps', '-r', 'once'],
                                 capture_output=True, text=True, timeout=20).stdout
            d = json.loads(out)
            with lock:
                last_gps.update(lat=d['latitude'], lon=d['longitude'],
                                acc_m=d.get('accuracy'), speed_mps=d.get('speed'),
                                ts=time.time())
        except Exception as e:
            print('GPS error:', e)
        time.sleep(3)


# ---------- simulator (tanpa HP) ----------
def sim_accel(mode):
    amp = {'fake': 0.05, 'walk': 1.5, 'spoof': 0.05}[mode]
    while True:
        with lock:
            accel_buf.append(G + random.gauss(0, amp))
        time.sleep(0.05)


def sim_gps(mode):
    t0 = time.time()
    speed = {'fake': 0.0, 'walk': 1.4, 'spoof': 8.0}[mode]
    while True:
        north_m = speed * (time.time() - t0)           # bergerak ke utara
        with lock:
            last_gps.update(lat=HOME[0] + north_m / 111000 + random.gauss(0, 0.00002),
                            lon=HOME[1] + random.gauss(0, 0.00002),
                            acc_m=8.0, speed_mps=speed, ts=time.time())
        time.sleep(3)


# ---------- pengolahan 1: RMS per jendela ----------
def rms_window():
    with lock:
        samples, accel_buf[:] = accel_buf[:], []
    if not samples:
        return None
    dyn = [s - G for s in samples]                     # buang gravitasi
    rms = math.sqrt(sum(d * d for d in dyn) / len(dyn))
    return rms, max(abs(d) for d in dyn), len(dyn)


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument('--sim', choices=['fake', 'walk', 'spoof'], help='mode simulasi tanpa HP')
    args = ap.parse_args()

    cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f'edge1-{UID}')
    cli.will_set(T_STATE, json.dumps({'state': 'offline'}), qos=1, retain=True)
    cli.connect(BROKER, PORT, keepalive=30)
    cli.loop_start()
    cli.publish(T_STATE, json.dumps({'state': 'online'}), qos=1, retain=True)

    if args.sim:
        threading.Thread(target=sim_accel, args=(args.sim,), daemon=True).start()
        threading.Thread(target=sim_gps, args=(args.sim,), daemon=True).start()
    else:
        threading.Thread(target=accel_reader_real, daemon=True).start()
        threading.Thread(target=gps_reader_real, daemon=True).start()

    hist = deque(maxlen=3)                             # moving average 3 jendela
    while True:
        time.sleep(WINDOW_S)
        w = rms_window()
        if w is None:
            continue
        rms, peak, n = w
        hist.append(rms)
        with lock:
            g = dict(last_gps)
        age = round(time.time() - g['ts'], 1) if g['ts'] else None
        msg = {
            'v': 1, 'device_id': 'edge1-android', 'ts': int(time.time()),
            'gps': {'lat': g['lat'], 'lon': g['lon'], 'acc_m': g['acc_m'],
                    'speed_mps': g['speed_mps'], 'age_s': age},
            'accel': {'rms': round(rms, 3), 'rms_smooth': round(sum(hist) / len(hist), 3),
                      'peak': round(peak, 3), 'samples': n, 'window_s': WINDOW_S},
        }
        cli.publish(T_TELE, json.dumps(msg), qos=0)


if __name__ == '__main__':
    main()