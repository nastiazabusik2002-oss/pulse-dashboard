"""
Пульс команди — авто-оновлювач + веб-сервер.

Раз на цикл (08:00 / 16:00 / 00:00 Europe/Kyiv) тягне свіжі дані з Zammad
(і, якщо налаштовано, зміни з Supabase), перегенеровує pulse.html
з шаблону і віддає його по HTTP на порту PORT (типово 8080).

Усі дані (включно з "Хто зробив"/"Зведення за період") тепер точні,
по-артиклові — рахуються з реальних артиклів у Zammad, а не з дешевого
підрахунку нових тікетів (той системно недораховував роботу над уже
наявними тікетами, типу фідбеків).

Тікети шукаємо ОДНИМ широким проходом на все вікно (discover_and_cache_tickets),
а не окремо на кожен день — вузький пошук "хто чіпав тікет саме в цей
день" губив артиклі на тікетах, які чіпали ще раз пізніше (last_contact_agent_at
зсувається вперед, і день, де реальний артикль є, більше не знаходиться).
Кеш — по ticket_id (tickets_v1.json), тікет перефетчується тільки якщо
його updated_at змінився. Розкладка по днях/агентах (bucket_articles_by_day)
рахується НАНОВО щоразу з кешованих артиклів — тому жоден день не застряє
з неправильним числом назавжди.

Кожен артикль зараховуємо тому, хто його реально написав (article.created_by),
незалежно від власника тікета. Дзвінки (type=="phone") і внутрішні нотатки
(type=="note") в "Листи"/"Фідбек"/"Чати" не йдуть — звірено з розробником
офіційної Zammad-статистики. Нотатка-передача в інший відділ — виняток:
"Інший відділ" це наша власна фіча, якої в офіційній статистиці немає.

/tickets/search мовчки обрізає результат на 200 записах незалежно від
limit — search_tickets сама гортає сторінки (page=).

Вікно днів прив'язане до PRECISE_ANCHOR_DATE і росте по одному дню за
раз (стеля WINDOW_DAYS_MAX), потім стає рухомим — це не дає дискавері
розтягнутись на місяці.
"""

import json
import os
import re
import threading
import time
import traceback
from concurrent.futures import ThreadPoolExecutor
from datetime import date, datetime, timedelta
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import urlparse, parse_qs
from zoneinfo import ZoneInfo

import requests

# ---------------------------------------------------------------------------
# Налаштування (з environment — секрети сюди не хардкодимо)
# ---------------------------------------------------------------------------

ZAMMAD_BASE = os.environ.get("ZAMMAD_BASE", "https://zammad.ttndev.com/api/v1")
ZAMMAD_TOKEN = os.environ["ZAMMAD_TOKEN"]

SUPABASE_URL = os.environ.get("SUPABASE_URL", "")
SUPABASE_SECRET_KEY = os.environ.get("SUPABASE_SECRET_KEY", "")
SUPABASE_SCHEDULE_TABLE = os.environ.get("SUPABASE_SCHEDULE_TABLE", "portal_data")
# portal_data — таблиця ключ/значення; весь портал (включно з розкладом)
# лежить під ключем "portal_v6" одним великим JSON-текстом. Схему
# підтверджено на реальному бекапі порталу: {"staff":[...з паролями...],
# "schedule": {"YYYY-MM": {"<staffId>": {"<day-без-нуля>": "<shiftTypeId|off|vacation>"}}},
# "shiftTypes": {"<teamId>": [{"id","label","time",...}]}}. Колонка value — TEXT,
# не jsonb, тому звузити вибірку на сервері (щоб оминути staff/паролі)
# неможливо — парсимо повний текст і одразу відкидаємо все, крім
# schedule/shiftTypes, ніде далі (лог, кеш) staff/паролі не потрапляють.
SUPABASE_SCHEDULE_KEY = os.environ.get("SUPABASE_SCHEDULE_KEY", "portal_v6")
SCHEDULE_TEAM_ID = "1"  # id команди всередині схеми порталу (не плутати зі schedule_id з TEAM)

# Simulator.company — реальний лічильник чатів (Zammad-евристика за назвою
# тікета "Chat – ..." майже нічого не ловить, тому справжнє джерело чатів —
# внутрішній дашборд-API Simulator). SIMULATOR_TOKEN — СЕСІЙНИЙ, згасає;
# коли протухне — див. README, як узяти новий з DevTools.
SIMULATOR_BASE = os.environ.get("SIMULATOR_BASE", "https://sim.simulator.company")
SIMULATOR_TOKEN = os.environ.get("SIMULATOR_TOKEN", "")
SIMULATOR_DASHBOARD_ID = os.environ.get(
    "SIMULATOR_DASHBOARD_ID", "9e0ac8cd-af6b-40e3-8d24-a0326c71e2ff"
)
SIMULATOR_NAME_ID = "78375144-5b27-45dc-90a2-21488d4c96b4"
SIMULATOR_CURRENCY_ID = "11294777"

PORT = int(os.environ.get("PORT", "8080"))
TZ = ZoneInfo("Europe/Kyiv")

APP_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(APP_DIR, "pulse_template.html")
OUTPUT_DIR = os.environ.get("OUTPUT_DIR", "/app/www")
OUTPUT_PATH = os.path.join(OUTPUT_DIR, "pulse.html")

CACHE_DIR = os.environ.get("CACHE_DIR", "/app/cache")
# Кеш по ticket_id (не по днях!) — див. коментар над discover_and_cache_tickets
# нижче про те, чому попередній підхід (окремий пошук на кожен день) губив
# артиклі на тікетах, які чіпали повторно пізніше.
TICKETS_CACHE_PATH = os.path.join(CACHE_DIR, "tickets_v1.json")
CHATS_CACHE_PATH = os.path.join(CACHE_DIR, "chats.json")

# Точні (по-артиклові) дані тепер живлять і "Хто зробив"/"Зведення за
# період" — раніше ці таблиці рахували дешевим способом (лише НОВІ тікети
# за день), що системно недораховувало роботу над старими тікетами (типу
# фідбеків) — на реальних даних розбіжність з офіційною статистикою Zammad
# була майже втричі. Тому все тепер по-артиклове, на одному й тому самому
# вікні.
#
# Вікно не тягнеться на повні WINDOW_DAYS_MAX від старту — прив'язане до
# ANCHOR_DATE (тиждень до старту реального використання) і "дозріває" само,
# по одному новому дню за раз, поки не впреться в стелю WINDOW_DAYS_MAX.
# Це не дає першому прогону роздутись на години.
WINDOW_DAYS_MAX = 40       # стеля — щоб "Минулий місяць" завжди мав повні дані
PRECISE_ANCHOR_DATE = date(2026, 8, 25)
CACHE_KEEP_DAYS = 45       # старіші дні з кешу прибираємо; має бути > WINDOW_DAYS_MAX

DEPT_GROUPS = {225: "Refund", 226: "Ticketing", 227: "Invol"}

# email -> (schedule_id, ім'я). schedule_id — той самий "id", що використовує
# pulse.html усередині AGENTS (не плутати з числовим id користувача в Zammad).
TEAM = [
    {"email": "a.nosal@ttn.global", "id": 1, "name": "Андріяна Носаль"},
    {"email": "k.malko@ttn.global", "id": 7, "name": "Христя Малко"},
    {"email": "s.hronska@ttn.global", "id": 2, "name": "Софа Гронська"},
    {"email": "m.ivasiuk@ttn.global", "id": 14, "name": "Маша Івасюк"},
    {"email": "a.volosianko@ttn.global", "id": 13, "name": "Настя Волосянко"},
    {"email": "m.podvadtsiatnyk@ttn.global", "id": 9, "name": "Марічка Подвадцятник"},
    {"email": "l.irza@ttn.global", "id": 126, "name": "Ліля Ірза"},
    {"email": "n.chykulaieva@ttn.global", "id": 6, "name": "Наталя Чикулаєва"},
    {"email": "n.teliatynska@ttn.global", "id": 10, "name": "Наталя Телятинська"},
    {"email": "n.hrytsan@ttn.global", "id": 8, "name": "Наталя Грицан"},
    {"email": "k.kozytska@ttn.global", "id": 4, "name": "Катя Козицька"},
    {"email": "v.vulchyn@ttn.global", "id": 12, "name": "Вова Вульчин"},
    {"email": "t.shvets@ttn.global", "id": 118, "name": "Таня Швець"},
    {"email": "n.krapivnoy@ttn.global", "id": 11, "name": "Нікіта Крапівной"},
    {"email": "o.radziminskyi@ttn.global", "id": 128, "name": "Саша Радзімінський"},
]

# email -> (actorId, title) в Simulator.company — зіставлено вручну по іменах
# з довідки, яку дав розробник Simulator.
SIMULATOR_ACTORS = {
    "a.nosal@ttn.global": ("caf82891-abdf-4078-9d97-7ff6f5bd6f29", "Andriiana Nosal"),
    "k.malko@ttn.global": ("723d135b-c42f-4a8b-8aca-7e3769b3a013", "Malko Khrystyna"),
    "s.hronska@ttn.global": ("0f90d20a-8699-41e5-9084-e6a27b19e92b", "Sofia Hronska"),
    "m.ivasiuk@ttn.global": ("51f99f07-4e40-4dc4-ac2b-51a7d3bd382e", "Mariia Ivasiuk"),
    "a.volosianko@ttn.global": ("e905307e-483a-4a04-a8bd-6511ca707f2b", "Anastasiia Volosianko"),
    "m.podvadtsiatnyk@ttn.global": ("96f8f983-976d-4e6b-b551-157ab044abac", "Mariia Podvadtsiatnyk"),
    "l.irza@ttn.global": ("47f015eb-252c-4af1-9d9c-740c2929e581", "Liliia Irza"),
    "n.chykulaieva@ttn.global": ("fe72fd82-d62b-4074-826b-be146165a3c7", "Chykulaieva Nataliia"),
    "n.teliatynska@ttn.global": ("24d54cb7-41d3-475c-af96-b93db683a055", "Teliatynska"),
    "n.hrytsan@ttn.global": ("d2bdeb5a-a769-49b6-a041-51509ac60d9e", "Nataliia Hrytsan"),
    "k.kozytska@ttn.global": ("b5a17dd7-9aad-409c-9714-4a1f4e7e0d68", "Kateryna Kozytska"),
    "v.vulchyn@ttn.global": ("cb96c123-a267-4705-abae-cddabcb0a7b9", "Vulchyn Volodymyr"),
    "t.shvets@ttn.global": ("d04a30b7-5817-498c-b4db-6bf251bc656a", "Tetiana Shvets"),
    "n.krapivnoy@ttn.global": ("46309104-eb31-4cf2-ba4d-3040c7e03010", "Nikita Krapivnoy"),
    "o.radziminskyi@ttn.global": ("3123465e-57bd-435b-81a0-8847e8496fb3", "Radziminskyi Oleksandr"),
}

# ---------------------------------------------------------------------------
# Zammad — HTTP-клієнт з ретраями
# ---------------------------------------------------------------------------

FETCH_WORKERS = 10  # паралельні запити тегів/артиклів по тікетах — послідовно
                     # на командний день (сотні тікетів) це займало б години
FETCH_CHUNK_SIZE = 1000  # тікетів за одну партію — обмежує пікову пам'ять
                          # при широкому дискавері (десятки тисяч тікетів)

_session = requests.Session()
_session.headers.update({"Authorization": f"Token token={ZAMMAD_TOKEN}"})
_adapter = requests.adapters.HTTPAdapter(pool_maxsize=FETCH_WORKERS + 2)
_session.mount("https://", _adapter)
_session.mount("http://", _adapter)


def zammad_get(path, params=None, retries=4):
    for attempt in range(retries):
        try:
            r = _session.get(ZAMMAD_BASE + path, params=params, timeout=20)
            r.raise_for_status()
            return r.json()
        except Exception:
            if attempt == retries - 1:
                raise
            time.sleep(1.5 * (attempt + 1))


def search_tickets(query, page_size=200):
    # /tickets/search мовчки обрізає результат на 200 записах незалежно
    # від limit (перевірено наживо) — без пагінації по page= для будь-якого
    # запиту, що знаходить понад 200 тікетів (типово командний запит за
    # день), частина тікетів просто губилась без жодної помилки.
    out = []
    page = 1
    while True:
        batch = zammad_get("/tickets/search", {"query": query, "limit": page_size, "page": page})
        out.extend(batch)
        if len(batch) < page_size:
            break
        page += 1
    return out


def find_zammad_user(email):
    users = zammad_get("/users/search", {"query": email, "limit": 3})
    return next((u for u in users if u.get("email", "").lower() == email.lower()), None)


# ---------------------------------------------------------------------------
# Точний (по-артикловий) аналіз усієї команди — тепер не "на кожен день
# окремо", а одним широким проходом на все вікно.
#
# Чому не по днях: попередній підхід шукав тікети вузько ("хто чіпав тікет
# САМЕ в цей день", по полю last_contact_agent_at у межах доби). Але якщо
# тікет чіпали ЩЕ РАЗ пізніше, це поле зсувається вперед — і вузький пошук
# за той давніший день більше НЕ знаходить тікет, хоча реальний артикль
# там є. Звірено з офіційною Zammad-статистикою: саме це — головна причина
# недорахунку в агентів з активним листуванням, що триває кілька днів
# (типу фідбеків).
#
# Тепер: один широкий пошук по всій компанії за [ANCHOR .. зараз], кеш
# артиклів по ticket_id (tickets_v1.json), тікет перефетчується тільки
# якщо його ticket.updated_at змінився з минулого разу. Розкладка по
# днях/агентах рахується НАНОВО щоразу з кешованих артиклів — тому вже
# "порахований" день ніколи не застряє з неправильним числом, навіть якщо
# тікет чіпнули знову вже після того, як день порахували.
#
# Кожен артикль зараховуємо тому, хто його РЕАЛЬНО написав (created_by),
# незалежно від того, хто зараз власник тікета. Дзвінки (type=="phone") і
# внутрішні нотатки (type=="note") в "Листи"/"Фідбек"/"Чати" не йдуть —
# перевірено на реальних агентах, це узгоджено з логікою офіційної
# статистики (яка рахує лише вхідні листи). Нотатка-передача в інший
# відділ (Refund/Ticketing/Invol) — виняток, бо "Інший відділ" це наша
# власна фіча, якої в офіційній статистиці взагалі немає.
# ---------------------------------------------------------------------------

def _article_kyiv_date(created_at_str):
    dt = datetime.fromisoformat(created_at_str.replace("Z", "+00:00"))
    return dt.astimezone(TZ).date().isoformat()


def _ticket_category(t, tags):
    title = t.get("title") or ""
    gid = t.get("group_id")
    if "feedback_all" in tags:
        return "feedback"
    if title.startswith("Chat –") or title.startswith("Chat -"):
        return "chat"
    if gid in DEPT_GROUPS:
        return "dept"
    return "listy"


def discover_and_cache_tickets(anchor_date, yesterday):
    anchor_local = datetime(anchor_date.year, anchor_date.month, anchor_date.day, tzinfo=TZ)
    now_utc = datetime.now(ZoneInfo("UTC"))
    anchor_str = anchor_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S")
    now_str = now_utc.strftime("%Y-%m-%dT%H:%M:%S")
    rng = f"[{anchor_str}Z TO {now_str}Z]"

    q_created = f"created_at:{rng}"
    q_touched = f"last_contact_agent_at:{rng}"

    tickets = {}
    for q in (q_created, q_touched):
        for t in search_tickets(q):
            tickets[t["id"]] = t
    print(f"[refresh] дискавері (широке вікно {anchor_date.isoformat()}..зараз): {len(tickets)} тікетів")

    ticket_cache = _load_json(TICKETS_CACHE_PATH, {})

    to_refresh = [
        (tid, t) for tid, t in tickets.items()
        if str(tid) not in ticket_cache or ticket_cache[str(tid)].get("updated_at") != t.get("updated_at")
    ]
    print(f"[refresh] тікетів на (пере)фетч: {len(to_refresh)} з {len(tickets)}")

    def _fetch_one(item):
        tid, t = item
        tags = zammad_get("/tags", {"object": "Ticket", "o_id": tid}).get("tags", [])
        cat = _ticket_category(t, tags)
        arts_raw = zammad_get(f"/ticket_articles/by_ticket/{tid}")
        arts = [
            {
                "created_by": a.get("created_by"),
                "sender": a.get("sender"),
                "type": a.get("type"),
                "created_at": a.get("created_at"),
            }
            for a in arts_raw
        ]
        return tid, {"updated_at": t.get("updated_at"), "cat": cat, "articles": arts}

    # партіями по FETCH_CHUNK_SIZE — ThreadPoolExecutor.map() ставить в
    # чергу ВСІ елементи одразу (усі Future одночасно в пам'яті), і на
    # десятках тисяч тікетів (усе широке вікно за раз) це разом з
    # артиклями кожного тікета виходило за ліміт пам'яті пода (OOM,
    # exit 137) — под падав і перезапускався, так і не дорахувавши.
    # Партіями пікове споживання обмежене розміром однієї партії.
    if to_refresh:
        for start in range(0, len(to_refresh), FETCH_CHUNK_SIZE):
            chunk = to_refresh[start:start + FETCH_CHUNK_SIZE]
            with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
                for tid, entry in pool.map(_fetch_one, chunk):
                    ticket_cache[str(tid)] = entry
            _save_json(TICKETS_CACHE_PATH, ticket_cache)
            print(f"[refresh] оброблено {min(start+FETCH_CHUNK_SIZE, len(to_refresh))} з {len(to_refresh)} тікетів")

    # прибираємо з кешу тікети, які давно випали з вікна дискавері — щоб
    # файл не ріс вічно
    cutoff = (yesterday - timedelta(days=CACHE_KEEP_DAYS)).isoformat()
    discovered_ids = set(str(tid) for tid in tickets.keys())
    for key in list(ticket_cache.keys()):
        if key in discovered_ids:
            continue
        upd = (ticket_cache[key].get("updated_at") or "")[:10]
        if upd and upd < cutoff:
            del ticket_cache[key]
    _save_json(TICKETS_CACHE_PATH, ticket_cache)

    return ticket_cache


def bucket_articles_by_day(ticket_cache, zammad_users, precise_days):
    login_to_email = {zu["login"]: email for email, zu in zammad_users.items()}
    days_set = set(precise_days)
    result = {
        d: {
            email: {"listy": 0, "feedback": 0, "chat": 0, "dept": 0, "notes": 0, "touches": 0, "tickets": set()}
            for email in zammad_users
        }
        for d in precise_days
    }

    for tid, entry in ticket_cache.items():
        cat = entry["cat"]
        for a in entry["articles"]:
            if a.get("sender") != "Agent":
                continue
            email = login_to_email.get(a.get("created_by"))
            if not email:
                continue
            ca = a.get("created_at") or ""
            if not ca:
                continue
            try:
                d = _article_kyiv_date(ca)
            except ValueError:
                continue
            if d not in days_set:
                continue
            r = result[d][email]
            r["touches"] += 1
            r["tickets"].add(tid)
            atype = a.get("type")
            if atype == "phone":
                continue
            if atype == "note":
                r["notes"] += 1
                if cat == "dept":
                    r["dept"] += 1
                continue
            r[cat] += 1

    return {
        d: {
            email: {
                "listy": r["listy"],
                "feedback": r["feedback"],
                "chatz": r["chat"],
                "dept": r["dept"],
                "notes": r["notes"],
                "touches": r["touches"],
                "tickets": len(r["tickets"]),
            }
            for email, r in day_data.items()
        }
        for d, day_data in result.items()
    }


# ---------------------------------------------------------------------------
# Зміни з Supabase. Схему (portal_data: key="sched_v6_t1", value=JSON з
# schedule/shiftTypes) підтверджено на реальному бекапі порталу — числа
# зійшлись 1-в-1 з тим, що вже було в pulse.html (перевірено вручну на
# кількох агентах/днях). М'який провал (порожній розклад), якщо Supabase
# не налаштовано чи недоступний — решта дашборду оновлюється як завжди.
# ---------------------------------------------------------------------------

_SHIFT_LABEL_RE = re.compile(r"[^\w\s]", re.UNICODE)


def _clean_shift_label(label):
    return _SHIFT_LABEL_RE.sub("", label).strip()


def fetch_team_schedule():
    """{"schedule": {...}, "shift_types": {teamId: {shiftId: {...}}}} з
    Supabase, або None якщо не налаштовано / недоступно."""
    if not SUPABASE_URL or not SUPABASE_SECRET_KEY:
        return None
    try:
        r = requests.get(
            f"{SUPABASE_URL}/rest/v1/{SUPABASE_SCHEDULE_TABLE}",
            params={"select": "value", "key": f"eq.{SUPABASE_SCHEDULE_KEY}"},
            headers={
                "apikey": SUPABASE_SECRET_KEY,
                "Authorization": f"Bearer {SUPABASE_SECRET_KEY}",
            },
            timeout=15,
        )
        r.raise_for_status()
        rows = r.json()
        if not rows:
            print(f"[shifts] ключ {SUPABASE_SCHEDULE_KEY!r} не знайдено в {SUPABASE_SCHEDULE_TABLE}")
            return None
        value = rows[0]["value"]
        # колонка value — TEXT з JSON-текстом усередині (не jsonb), тому
        # парсимо самі; одразу після цього staff/паролі більше ніде не
        # використовуються й не потрапляють ні в лог, ні на диск.
        if isinstance(value, str):
            value = json.loads(value)
    except Exception:
        print("[shifts] не вдалось отримати розклад з Supabase:")
        traceback.print_exc()
        return None

    # value — цілий документ порталу ({staff, schedule, shiftTypes, ...}).
    # Беремо тільки schedule/shiftTypes; якщо колись прийде вже звужений
    # об'єкт (лише "schedule") — теж підтримуємо.
    schedule = value.get("schedule", value) if isinstance(value, dict) else {}
    shift_types_raw = value.get("shiftTypes", {}) if isinstance(value, dict) else {}
    shift_types = {
        tid: {st["id"]: st for st in lst} for tid, lst in shift_types_raw.items()
    }
    if not shift_types.get(SCHEDULE_TEAM_ID):
        print(f"[shifts] у value нема shiftTypes для команди {SCHEDULE_TEAM_ID} — усі зміни підуть як OFF")
    return {"schedule": schedule, "shift_types": shift_types}


def shift_str_for(team_schedule, staff_id, date_str):
    """Рядок зміни у форматі, який уже розуміє pulse.html
    (SHIFT_CAT дивиться на префікс: 'ЧАТИ'/'ЛИСТИ'/'ФІДБЕК'/'Нічна'/'Старший')."""
    y, m, d = date_str.split("-")
    month_key = f"{y}-{m}"
    day_key = str(int(d))
    raw = team_schedule["schedule"].get(month_key, {}).get(str(staff_id), {}).get(day_key)
    if raw is None or raw == "off":
        return "OFF"
    if raw == "vacation":
        return "VAC"
    st = team_schedule["shift_types"].get(SCHEDULE_TEAM_ID, {}).get(raw)
    if not st:
        return "OFF"
    label = _clean_shift_label(st.get("label", ""))
    time = st.get("time", "")
    return f"{label}@{time}" if time else label


# ---------------------------------------------------------------------------
# Реальні чати з Simulator.company. Кидає виняток при збої (протухлий токен,
# мережа) — виклик сам вирішує, чи кешувати день (щоб не "заморозити" 0
# назавжди після тимчасового збою).
# ---------------------------------------------------------------------------

def fetch_chats_for_date(date_str):
    """{schedule_id: кількість чатів за день} по всій команді одним запитом,
    або {} якщо SIMULATOR_TOKEN не задано (Simulator просто не підключений)."""
    if not SIMULATOR_TOKEN:
        return {}

    y, m, d = (int(x) for x in date_str.split("-"))
    day_start = datetime(y, m, d, tzinfo=TZ)
    day_end = day_start + timedelta(days=1) - timedelta(milliseconds=1)

    accounts = [
        {
            "actorId": actor_id,
            "account": {},
            "nameId": SIMULATOR_NAME_ID,
            "currencyId": SIMULATOR_CURRENCY_ID,
            "actor": {"id": actor_id, "title": title},
            "accountType": "fact",
            "incomeType": "total",
            "color": "#000000",
        }
        for actor_id, title in SIMULATOR_ACTORS.values()
    ]

    r = requests.post(
        f"{SIMULATOR_BASE}/api/1.0/dashboards/{SIMULATOR_DASHBOARD_ID}",
        params={
            "from": int(day_start.timestamp() * 1000),
            "to": int(day_end.timestamp() * 1000),
            "interval": "day",
            "timezoneOffset": -180,
        },
        headers={
            "Authorization": f"Bearer {SIMULATOR_TOKEN}",
            "Content-Type": "application/json",
        },
        json={"source": {"accounts": accounts, "counterType": "amount", "chartType": "bar"}},
        timeout=20,
    )
    r.raise_for_status()
    rows = r.json().get("data", [])

    actor_to_email = {v[0]: k for k, v in SIMULATOR_ACTORS.items()}
    email_to_id = {m["email"]: m["id"] for m in TEAM}
    out = {}
    for row in rows:
        email = actor_to_email.get(row.get("actorId"))
        sid = email_to_id.get(email)
        if sid is not None:
            out[str(sid)] = row.get("value", 0)
    return out


# ---------------------------------------------------------------------------
# Кеш на диску
# ---------------------------------------------------------------------------

def _load_json(path, default):
    try:
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _save_json(path, data):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False)
    os.replace(tmp, path)


def _date_range(end_date, days):
    return [(end_date - timedelta(days=i)).isoformat() for i in range(days - 1, -1, -1)]


def _date_range_between(from_str, to_str):
    d0 = date.fromisoformat(from_str)
    d1 = date.fromisoformat(to_str)
    return [(d0 + timedelta(days=i)).isoformat() for i in range((d1 - d0).days + 1)]


def _prune_cache(cache, keep_days_before):
    cutoff = keep_days_before.isoformat()
    for d in list(cache.keys()):
        if d < cutoff:
            del cache[d]


# ---------------------------------------------------------------------------
# Головна функція оновлення
# ---------------------------------------------------------------------------

def regenerate():
    print(f"[refresh] старт {datetime.now(TZ).isoformat()}")
    yesterday = datetime.now(TZ).date() - timedelta(days=1)

    days_since_anchor = (yesterday - PRECISE_ANCHOR_DATE).days + 1
    window_len = max(1, min(WINDOW_DAYS_MAX, days_since_anchor))
    precise_days = _date_range(yesterday, window_len)
    recent_days = precise_days[-7:]  # останні 7 днів з того самого вікна —
                                      # для AGENTS.daily / "week"-картки агента

    # довідник zammad user_id/login по email — тягнемо раз на запуск
    zammad_users = {}
    for member in TEAM:
        try:
            u = find_zammad_user(member["email"])
            if u:
                zammad_users[member["email"]] = {"id": u["id"], "login": u["login"]}
            else:
                print(f"[refresh] не знайшла в Zammad: {member['email']}")
        except Exception:
            print(f"[refresh] помилка пошуку {member['email']}:")
            traceback.print_exc()

    # --- точні дані (для AGENTS.daily / yday_*) — широке дискавері на все
    # вікно + розкладка по днях наново з кешу тікетів щоразу ---
    try:
        anchor_for_discovery = date.fromisoformat(precise_days[0])
        ticket_cache = discover_and_cache_tickets(anchor_for_discovery, yesterday)
        precise_cache = bucket_articles_by_day(ticket_cache, zammad_users, precise_days)
    except Exception:
        print("[refresh] помилка дискавері/розкладки по днях:")
        traceback.print_exc()
        return

    # --- зміни (один запит на весь розклад команди, далі рахуємо по днях у Python) ---
    team_schedule = fetch_team_schedule()
    if team_schedule:
        shifts_by_day = {
            d: {m["email"].lower(): shift_str_for(team_schedule, m["id"], d) for m in TEAM}
            for d in recent_days
        }
        # той самий розклад, але на все precise_days (анкероване вікно) і
        # ключем schedule_id (як у RANGE_DATA) — для "Зведення за період",
        # щоб пресети типу "Минулий місяць" рахували К-сть змін вірно.
        shifts_range = {
            d: {str(m["id"]): shift_str_for(team_schedule, m["id"], d) for m in TEAM}
            for d in precise_days
        }
    else:
        shifts_by_day = {d: {} for d in recent_days}
        shifts_range = {d: {} for d in precise_days}

    # --- реальні чати з Simulator (перекриють Zammad-евристику в RANGE_DATA) ---
    chats_cache = _load_json(CHATS_CACHE_PATH, {})
    if SIMULATOR_TOKEN:
        for d in precise_days:
            if d in chats_cache:
                continue
            print(f"[refresh] тягну чати з Simulator за {d}")
            try:
                chats_cache[d] = fetch_chats_for_date(d)
            except Exception:
                print(f"[refresh] не вдалось отримати чати за {d} (токен протух?):")
                traceback.print_exc()
                continue
            _save_json(CHATS_CACHE_PATH, chats_cache)
        _prune_cache(chats_cache, yesterday - timedelta(days=CACHE_KEEP_DAYS))
        _save_json(CHATS_CACHE_PATH, chats_cache)

    build_html(precise_cache, shifts_by_day, chats_cache, shifts_range, recent_days, precise_days, yesterday)
    print(f"[refresh] готово {datetime.now(TZ).isoformat()}")


def replace_const(name, value_json, text):
    pattern = re.compile(r"const " + name + r" = .*?;", re.S)
    replacement = f"const {name} = {value_json};"
    new_text, n = pattern.subn(replacement, text, count=1)
    if n == 0:
        raise RuntimeError(f"не знайшла `const {name} = ...;` у шаблоні")
    return new_text


def build_html(precise_cache, shifts_by_day, chats_cache, shifts_range, recent_days, precise_days, yesterday):
    agents = []
    for member in TEAM:
        daily = []
        for d in recent_days:
            r = precise_cache.get(d, {}).get(member["email"], {})
            shift = shifts_by_day.get(d, {}).get(member["email"].lower(), "OFF")
            daily.append({
                "date": d,
                "touches": r.get("touches", 0),
                "tickets": r.get("tickets", 0),
                "shift": shift,
            })

        y = precise_cache.get(recent_days[-1], {}).get(member["email"], {})
        week_touches = sum(x["touches"] for x in daily)
        week_tickets = sum(x["tickets"] for x in daily)
        active_days = sum(1 for x in daily if x["touches"] > 0)
        yshift = shifts_by_day.get(recent_days[-1], {}).get(member["email"].lower(), "OFF")

        agents.append({
            "id": member["id"],
            "name": member["name"],
            "email": member["email"],
            "yday_shift": yshift,
            "yday_listy": y.get("listy", 0),
            "yday_feedback": y.get("feedback", 0),
            "yday_chats_z": y.get("chatz", 0),
            "yday_dept": y.get("dept", 0),
            "yday_notes": y.get("notes", 0),
            "yday_touches_raw": y.get("touches", 0),
            "yday_tickets_raw": y.get("tickets", 0),
            "week_touches": week_touches,
            "week_tickets": week_tickets,
            "active_days": active_days,
            "daily": daily,
        })

    # {date: {schedule_id: {...}}} — тепер напряму з точних (по-артиклових)
    # даних (precise_cache), а не з дешевого тікет-рівневого підрахунку,
    # який системно недораховував роботу над уже наявними тікетами (типу
    # фідбеків). Де є реальні чати з Simulator за цей день — підміняємо
    # ними "chatz".
    range_data = {}
    for d in precise_days:
        day_chats = chats_cache.get(d, {})
        merged = {}
        for member in TEAM:
            r = precise_cache.get(d, {}).get(member["email"], {})
            cats = {
                "listy": r.get("listy", 0),
                "feedback": r.get("feedback", 0),
                "chatz": r.get("chatz", 0),
                "dept": r.get("dept", 0),
            }
            sid = str(member["id"])
            if sid in day_chats:
                cats["chatz"] = day_chats[sid]
            merged[sid] = cats
        range_data[d] = merged

    range_min, range_max = precise_days[0], precise_days[-1]

    html = open(TEMPLATE_PATH, "r", encoding="utf-8").read()

    html = replace_const("AGENTS", json.dumps(agents, ensure_ascii=False, separators=(",", ":")), html)
    html = replace_const("RANGE_DATA", json.dumps(range_data, ensure_ascii=False, separators=(",", ":")), html)
    html = replace_const("RANGE_MIN", json.dumps(range_min), html)
    html = replace_const("RANGE_MAX", json.dumps(range_max), html)
    html = replace_const("SHIFTS_RANGE", json.dumps(shifts_range, ensure_ascii=False, separators=(",", ":")), html)

    now_str = datetime.now(TZ).strftime("%Y-%m-%d, %H:%M")
    html = re.sub(r"Оновлено: [^<]*\(Europe/Kiev\)", f"Оновлено: {now_str} (Europe/Kiev)", html)

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)


# ---------------------------------------------------------------------------
# Планувальник: перше оновлення одразу при старті, далі — о 08:00/16:00/00:00
# за київським часом.
# ---------------------------------------------------------------------------

REFRESH_HOURS = {8, 16, 0}


def scheduler_loop():
    try:
        regenerate()
    except Exception:
        print("[refresh] помилка першого оновлення:")
        traceback.print_exc()

    last_fired_hour = None
    while True:
        now = datetime.now(TZ)
        if now.hour in REFRESH_HOURS and now.minute == 0 and last_fired_hour != now.hour:
            last_fired_hour = now.hour
            try:
                regenerate()
            except Exception:
                print("[refresh] помилка планового оновлення:")
                traceback.print_exc()
        time.sleep(20)


# ---------------------------------------------------------------------------
# HTTP-сервер: віддає pulse.html, health-чек для k8s, ручний /refresh
# ---------------------------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        pass  # не засмічуємо логи запитами до health-чеку

    def do_GET(self):
        if self.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        if os.path.exists(OUTPUT_PATH):
            with open(OUTPUT_PATH, "rb") as f:
                body = f.read()
            self.send_response(200)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Cache-Control", "no-store")
            self.end_headers()
            self.wfile.write(body)
        else:
            self.send_response(503)
            self.end_headers()
            self.wfile.write(b"dashboard is not generated yet, first refresh still running")

    def do_POST(self):
        parsed = urlparse(self.path)
        if parsed.path == "/refresh":
            threading.Thread(target=regenerate, daemon=True).start()
            self.send_response(202)
            self.end_headers()
            self.wfile.write(b"refresh triggered")
        elif parsed.path == "/refresh-range":
            # примусово скидає з кешу тікети, що мають артиклі в цьому
            # діапазоні дат (перефетчує їх наново, навіть якщо
            # ticket.updated_at не змінився) — для ручної перевірки. У
            # звичайному режимі це НЕ потрібно: розкладка по днях і так
            # рахується наново з кешу тікетів щоразу (жоден день більше не
            # застряє з неправильним числом сам по собі).
            # POST /refresh-range?from=2026-09-01&to=2026-09-10
            qs = parse_qs(parsed.query)
            frm = qs.get("from", [None])[0]
            to = qs.get("to", [None])[0]
            if not frm or not to:
                self.send_response(400)
                self.end_headers()
                self.wfile.write(b"potribno from i to (YYYY-MM-DD)")
                return

            def _refresh_range():
                days = set(_date_range_between(frm, to))
                ticket_cache = _load_json(TICKETS_CACHE_PATH, {})
                dropped = 0
                for tid in list(ticket_cache.keys()):
                    entry = ticket_cache[tid]
                    for a in entry.get("articles", []):
                        ca = a.get("created_at") or ""
                        try:
                            d = _article_kyiv_date(ca)
                        except ValueError:
                            continue
                        if d in days:
                            del ticket_cache[tid]
                            dropped += 1
                            break
                _save_json(TICKETS_CACHE_PATH, ticket_cache)
                print(f"[refresh-range] скинула {dropped} тікетів з кешу за {frm}..{to}")
                regenerate()

            threading.Thread(target=_refresh_range, daemon=True).start()
            self.send_response(202)
            self.end_headers()
            self.wfile.write(f"refresh-range triggered: {frm}..{to}".encode())
        else:
            self.send_response(404)
            self.end_headers()


if __name__ == "__main__":
    threading.Thread(target=scheduler_loop, daemon=True).start()
    print(f"[app] слухаю на порту {PORT}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
