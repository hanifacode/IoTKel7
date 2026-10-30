# edge2_server.py - Edge 2 (laptop): pengolahan data + mesin keputusan
import json, math, time
import paho.mqtt.client as mqtt

BROKER, PORT = 'broker.hivemq.com', 1883
UID = 'kelompok_07'
BASE = f'otwtracker/{UID}'
HOME = [-7.06044, 110.43661]

CFG = dict(HOME_RADIUS=2, LEAVE_RADIUS=15, MAX_ACC=50, MAX_AGE=15,
           STILL_RMS=0.15, WALK_RMS=0.2, VEHICLE_RMS=0.2,
           WALK_SPEED=0.1, VEHICLE_SPEED=1.0,
           N_CONFIRM=1, GRACE_S=90, TIMEOUT_S=300)

def haversine(lat1, lon1, lat2, lon2):
    R = 6371000.0
    p1, p2 = math.radians(lat1), math.radians(lat2)
    dp, dl = p2 - p1, math.radians(lon2 - lon1)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return 2 * R * math.asin(math.sqrt(a))

def classify(rms, speed):
    if speed >= CFG['VEHICLE_SPEED']:
        return 'VEHICLE' if rms >= CFG['VEHICLE_RMS'] else 'SPOOF_SUSPECT'
    if speed >= CFG['WALK_SPEED'] and rms >= CFG['WALK_RMS']:
        return 'WALKING'
    if speed < CFG['WALK_SPEED'] and rms < CFG['STILL_RMS']:
        return 'STILL'
    return 'UNCERTAIN'

class OtwTracker:
    def __init__(self):
        self.reset()

    def reset(self):
        self.state, self.reason, self.t_claim = 'IDLE', None, None
        self.moving = self.still = self.spoof = 0

    def claim(self):
        self.reset()
        self.state, self.t_claim = 'PENDING', time.time()

    def update(self, tele):
        gps, acc = tele['gps'], tele['accel']
        info = {'dist_m': None, 'speed_mps': None, 'motion': None}
        good = (gps.get('lat') is not None
                and (gps.get('acc_m') or 999) <= CFG['MAX_ACC']
                and (gps.get('age_s') or 0) <= CFG['MAX_AGE'])
        if good:
            global HOME
            if HOME is None:
                HOME = [gps['lat'], gps['lon']]

            d_home = haversine(gps['lat'], gps['lon'], HOME[0], HOME[1])
            speed = gps.get('speed_mps') or 0.0
            motion = classify(acc['rms_smooth'], speed)
            leave = d_home >= CFG['LEAVE_RADIUS']
            home = d_home <= CFG['HOME_RADIUS']
            self.moving = self.moving + 1 if leave and motion in ('WALKING', 'VEHICLE') else 0
            self.still = self.still + 1 if home and motion == 'STILL' else 0
            self.spoof = self.spoof + 1 if motion == 'SPOOF_SUSPECT' else 0
            info = {'dist_m': round(d_home), 'speed_mps': round(speed, 2), 'motion': motion, 'gps_accuracy_m': gps.get('acc_m')}
        self._decide()
        return info

    def _decide(self):
        if self.state != 'PENDING':
            return
        n, elapsed = CFG['N_CONFIRM'], time.time() - self.t_claim
        if self.moving >= n:
            self._set('TRUE_OTW', 'LEFT_HOME_AND_MOVING')
        elif self.spoof >= n:
            self._set('FAKE_OTW', 'GPS_SPOOF_SUSPECTED')
        elif elapsed >= CFG['GRACE_S'] and self.still >= n:
            self._set('FAKE_OTW', 'STILL_AT_HOME')
        elif elapsed >= CFG['TIMEOUT_S']:
            self._set('FAKE_OTW', 'TIMEOUT_NOT_LEFT')

    def _set(self, state, reason):
        self.state, self.reason = state, reason

tracker = OtwTracker()
snap = {'dist_m': None, 'speed_mps': None, 'motion': None, 'rms': None, 'gps_accuracy_m': None}

def publish_status(cli):
    msg = dict(v=1, ts=int(time.time()), state=tracker.state, reason=tracker.reason,
               elapsed_s=int(time.time() - tracker.t_claim) if tracker.t_claim else None, **snap)
    cli.publish(f'{BASE}/status', json.dumps(msg), qos=1, retain=True)

def publish_alert(cli):
    ok = tracker.state == 'TRUE_OTW'
    msg = {'v': 1, 'ts': int(time.time()), 'type': tracker.state,
           'level': 'ok' if ok else 'danger', 'reason': tracker.reason, 'dist_m': snap['dist_m'],
           'message': 'Valid! Kamu beneran sudah berangkat.' if ok
                      else f'Terdeteksi BOONG ({tracker.reason})'}
    cli.publish(f'{BASE}/alert', json.dumps(msg), qos=1)

def on_connect(cli, userdata, flags, reason_code, properties):
    print('Edge 2 terhubung:', reason_code)
    cli.subscribe([(f'{BASE}/telemetry', 0), (f'{BASE}/cmd/claim', 1)])
    
    # Langsung aktifkan klaim perjalanan secara otomatis saat terhubung
    tracker.claim()
    publish_status(cli)

def on_message(cli, userdata, m):
    print(f"Pesan masuk di topik: {m.topic}") # <-- Tambahkan baris ini untuk mengecek
    try:
        data = json.loads(m.payload)
    except ValueError:
        return
    if m.topic.endswith('/cmd/claim'):
        if data.get('action') == 'start':
            tracker.claim()
        elif data.get('action') == 'cancel':
            tracker.reset()
        publish_status(cli)
        return
    before = tracker.state
    try:
        info = tracker.update(data)
        snap['rms'] = data['accel']['rms_smooth']
    except (KeyError, TypeError) as e:
        print("Error parsing data:", e) # <-- Supaya tahu jika format datanya meleset
        return
    if info['dist_m'] is not None:
        snap.update(info)
    if tracker.state != before and tracker.state in ('TRUE_OTW', 'FAKE_OTW'):
        publish_alert(cli)
    publish_status(cli)

cli = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id=f'edge2-{UID}')
cli.on_connect, cli.on_message = on_connect, on_message
cli.connect(BROKER, PORT, keepalive=30)
cli.loop_forever()