"""
Пульс команди — авто-оновлювач + веб-сервер.

Раз на цикл (08:00 / 16:00 / 00:00 Europe/Kyiv) тягне свіжі дані з Zammad
(і, якщо налаштовано, зміни з Supabase), перегенеровує pulse.html
з шаблону і віддає його по HTTP на порту PORT (типово 8080).

Усі дані (включно з "Хто зробив"/"Зведення за період") тепер точні,
по-артиклові — рахуються з реальних артиклів у Zammad, а не з дешевого
підрахунку нових тікетів (той системно недораховував роботу над уже
наявними тікетами, типу фідбеків).

Рахуємо одним проходом на всю команду за день (analyze_team_day), а не
по кожному агенту окремо — бо власник тікета в Zammad міняється протягом
дня, і "чий тікет" != "хто реально написав". Кожен артикль зараховуємо
тому, хто його написав (article.created_by), звірено з офіційною
Zammad-статистикою вручну. Дзвінки (type=="phone", лог розмови без
тексту) в підрахунок не йдуть, як і внутрішні нотатки.

/tickets/search мовчки обрізає результат на 200 записах незалежно від
limit — без пагінації (page=) частина тікетів губилась без помилки,
особливо в командному запиті за день. Тепер search_tickets гортає
сторінки сама.

Вікно днів прив'язане до PRECISE_ANCHOR_DATE і росте по одному дню за
раз, поки не впреться в стелю WINDOW_DAYS_MAX — це не дає першому
прогону роздутись на години.

Кеш проміжних результатів лежить у /app/cache — тому кожне наступне
оновлення рахує наново тільки НОВІ дні, а не все вікно заново.
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
PRECISE_CACHE_PATH = os.path.join(CACHE_DIR, "precise_v4.json")
# v2 -> v3: у v2 "Інший відділ" завжди виходив 0 — нотатки-передачі
# (agent пише причину й переносить тікет у Refund/Ticketing/Invol) йшли
# виключно як type=="note", а такі повністю виключались з підрахунку.
# v3 -> v4: межі доби рахувались так, ніби Zammad-час уже київський —
# насправді created_at в UTC, а Київ восени +3 (EEST). Перші ~3 години
# нічної зміни (00:00-03:00 за Києвом) через це йшли в підрахунок
# ПОПЕРЕДНЬОГО дня — найпомітніше саме для нічних змін.
# Кожна нова назва файлу знову примушує пересчитати все заново.
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
# Точний (по-артикловий) аналіз ВСІЄЇ команди за один день, одним проходом.
#
# Раніше рахували на кожного агента окремо (тікети, де він owner/creator),
# але власник тікета в Zammad міняється протягом дня (тікет перекидають між
# агентами, повертають у чергу) — тож частина реальних відповідей губилась
# або приписувалась не тому агенту. Офіційна Zammad-статистика (з якою
# звіряли вручну) рахує "хто фактично написав" по історії тікета, а не по
# поточному власнику. Тому тепер: спершу одним широким запитом (по всій
# команді разом) знаходимо ВСІ тікети, яких хтось із команди торкався за
# день, а потім кожен артикль зараховуємо тому, хто його РЕАЛЬНО написав
# (article.created_by), незалежно від того, хто зараз власник тікета.
#
# Дзвінки (type == "phone" — системний лог розмови, не написаний текст)
# так само не рахуємо як артикль, як і внутрішні нотатки (type == "note") —
# перевірено на реальних агентах з великою часткою дзвінків: без цього
# виключення їх денна сума була в 3-4 рази більша за офіційну статистику.
#
# Пошук тікетів НЕ обмежений owner_id:(наші 15) — перевірено наживо: якщо
# тікет ескалюють/передають комусь поза командою (навіть тимчасово), він
# зникає з такого пошуку разом з усіма артиклями наших агентів на ньому.
# Тому дискавері тепер по всій компанії за день, а фільтр "чи це наш
# агент" застосовується вже на рівні автора артикля нижче. Це набагато
# дорожче (тисячі тікетів на день замість сотень) — свідомий компроміс:
# перший бекфіл вікна після цього фіксу займе години, а не хвилини, зате
# рахує без цієї діри.
def analyze_team_day(zammad_users, date_str):
    # Межі доби рахуємо в київському часі й конвертуємо в UTC — Zammad
    # зберігає created_at в UTC, а Київ восени +3 (EEST). Раніше межі дня
    # рахувались так, ніби d0/d1 вже UTC (тобто зі зсувом на ці 2-3
    # години) — для денних змін це майже непомітно, але для нічної зміни
    # (яка якраз триває через північ за Києвом) перші ~3 години нічної
    # роботи (00:00–03:00 за Києвом) помилково йшли в підрахунок
    # ПОПЕРЕДНЬОГО дня.
    y, m, d = (int(x) for x in date_str.split("-"))
    d0_local = datetime(y, m, d, tzinfo=TZ)
    d1_local = d0_local + timedelta(days=1)
    d0_str = d0_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S")
    d1_str = d1_local.astimezone(ZoneInfo("UTC")).strftime("%Y-%m-%dT%H:%M:%S")
    rng = f"[{d0_str}Z TO {d1_str}Z]"

    q_created = f"created_at:{rng}"
    q_touched = f"last_contact_agent_at:{rng}"

    tickets = {}
    for q in (q_created, q_touched):
        for t in search_tickets(q):
            tickets[t["id"]] = t

    def _tags_for(tid):
        return tid, zammad_get("/tags", {"object": "Ticket", "o_id": tid}).get("tags", [])

    ticket_cat = {}
    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        for tid, tags in pool.map(_tags_for, tickets.keys()):
            t = tickets[tid]
            title = t.get("title") or ""
            gid = t.get("group_id")
            if "feedback_all" in tags:
                ticket_cat[tid] = "feedback"
            elif title.startswith("Chat –") or title.startswith("Chat -"):
                ticket_cat[tid] = "chat"
            elif gid in DEPT_GROUPS:
                ticket_cat[tid] = "dept"
            else:
                ticket_cat[tid] = "listy"

    login_to_email = {zu["login"]: email for email, zu in zammad_users.items()}
    per_agent = {
        email: {"listy": 0, "feedback": 0, "chat": 0, "dept": 0, "notes": 0, "touches": 0, "tickets": set()}
        for email in zammad_users
    }

    def _articles_for(tid):
        return tid, zammad_get(f"/ticket_articles/by_ticket/{tid}")

    with ThreadPoolExecutor(max_workers=FETCH_WORKERS) as pool:
        articles_by_ticket = list(pool.map(_articles_for, tickets.keys()))

    for tid, arts in articles_by_ticket:
        for a in arts:
            if a.get("sender") != "Agent":
                continue
            email = login_to_email.get(a.get("created_by"))
            if not email:
                continue  # артикль не від когось із нашої команди (тімлід/бот/інтеграція)
            ca = a.get("created_at", "")
            if not (d0_str <= ca < d1_str):
                continue
            r = per_agent[email]
            r["touches"] += 1
            r["tickets"].add(tid)
            atype = a.get("type")
            if atype == "phone":
                continue
            if atype == "note":
                r["notes"] += 1
                # передача в інший відділ оформлюється саме внутрішньою
                # нотаткою (агент пише причину і переносить тікет у
                # Refund/Ticketing/Invol) — це реальна "передача", не
                # службова робоча нотатка, тому саме тут notes рахуємо
                # ще й як dept, а не пропускаємо.
                if ticket_cat[tid] == "dept":
                    r["dept"] += 1
                continue
            r[ticket_cat[tid]] += 1

    return {
        email: {
            "listy": r["listy"],
            "feedback": r["feedback"],
            "chatz": r["chat"],
            "dept": r["dept"],
            "notes": r["notes"],
            "touches": r["touches"],
            "tickets": len(r["tickets"]),
        }
        for email, r in per_agent.items()
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

    precise_cache = _load_json(PRECISE_CACHE_PATH, {})

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

    # --- точні дані (для AGENTS.daily / yday_*) ---
    for d in precise_days:
        if d in precise_cache:
            continue
        print(f"[refresh] тягну точні дані за {d}")
        try:
            precise_cache[d] = analyze_team_day(zammad_users, d)
        except Exception:
            print(f"[refresh] помилка analyze_team_day {d}:")
            traceback.print_exc()
            continue
        _save_json(PRECISE_CACHE_PATH, precise_cache)

    _prune_cache(precise_cache, yesterday - timedelta(days=CACHE_KEEP_DAYS))
    _save_json(PRECISE_CACHE_PATH, precise_cache)

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
        if self.path == "/refresh":
            threading.Thread(target=regenerate, daemon=True).start()
            self.send_response(202)
            self.end_headers()
            self.wfile.write(b"refresh triggered")
        else:
            self.send_response(404)
            self.end_headers()


if __name__ == "__main__":
    threading.Thread(target=scheduler_loop, daemon=True).start()
    print(f"[app] слухаю на порту {PORT}")
    ThreadingHTTPServer(("0.0.0.0", PORT), Handler).serve_forever()
