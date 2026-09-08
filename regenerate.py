"""
Пульс команди — генератор index.html для GitHub Actions + Netlify.

Запускається за розкладом (.github/workflows/update-dashboard.yml), тягне
свіжі дані з Zammad (+ Supabase-розклад, + Simulator-чати), перегенеровує
index.html з шаблону і завершується. Дальше вже сам workflow комітить
index.html і кеш назад у репозиторій — Netlify підхоплює автоматично.

Кеш проміжних результатів лежить у cache/ і теж комітиться в git — тому
кожен наступний запуск рахує наново тільки НОВІ дні, а не всі 7/40 днів
заново.
"""

import json
import os
import re
import time
import traceback
from datetime import date, datetime, timedelta
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
SUPABASE_SCHEDULE_KEY = os.environ.get("SUPABASE_SCHEDULE_KEY", "portal_v6")
SCHEDULE_TEAM_ID = "1"  # id команди всередині схеми порталу (не плутати зі schedule_id з TEAM)

SIMULATOR_BASE = os.environ.get("SIMULATOR_BASE", "https://sim.simulator.company")
SIMULATOR_TOKEN = os.environ.get("SIMULATOR_TOKEN", "")
SIMULATOR_DASHBOARD_ID = os.environ.get(
    "SIMULATOR_DASHBOARD_ID", "9e0ac8cd-af6b-40e3-8d24-a0326c71e2ff"
)
SIMULATOR_NAME_ID = "78375144-5b27-45dc-90a2-21488d4c96b4"
SIMULATOR_CURRENCY_ID = "11294777"

TZ = ZoneInfo("Europe/Kyiv")

REPO_DIR = os.path.dirname(os.path.abspath(__file__))
TEMPLATE_PATH = os.path.join(REPO_DIR, "pulse_template.html")
OUTPUT_PATH = os.path.join(REPO_DIR, "index.html")

CACHE_DIR = os.path.join(REPO_DIR, "cache")
PRECISE_CACHE_PATH = os.path.join(CACHE_DIR, "precise.json")
RANGE_CACHE_PATH = os.path.join(CACHE_DIR, "range.json")
CHATS_CACHE_PATH = os.path.join(CACHE_DIR, "chats.json")

PRECISE_WINDOW_DAYS = 7   # скільки днів тримаємо точну (по-артиклову) статистику
RANGE_WINDOW_DAYS = 40    # скільки днів тримаємо категоризацію тікетів + розклад
                          # (40, а не місяць, — щоб "Минулий місяць" 1-го числа
                          # завжди мав повні дані за весь попередній місяць)
CACHE_KEEP_DAYS = 45      # старіші дні з кешу прибираємо; має бути > RANGE_WINDOW_DAYS

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

_session = requests.Session()
_session.headers.update({"Authorization": f"Token token={ZAMMAD_TOKEN}"})


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


def search_tickets(query, limit=200):
    return zammad_get("/tickets/search", {"query": query, "limit": limit})


def find_zammad_user(email):
    users = zammad_get("/users/search", {"query": email, "limit": 3})
    return next((u for u in users if u.get("email", "").lower() == email.lower()), None)


# ---------------------------------------------------------------------------
# Точний (по-артикловий) аналіз одного агента за один день.
# ---------------------------------------------------------------------------

def analyze_agent_day(user_id, login, date_str):
    y, m, d = (int(x) for x in date_str.split("-"))
    d0 = date(y, m, d)
    d1 = d0 + timedelta(days=1)
    rng = f"[{d0.isoformat()}T00:00:00Z TO {d1.isoformat()}T00:00:00Z]"

    q_created = f"(created_by_id:{user_id} OR owner_id:{user_id}) AND created_at:{rng}"
    q_touched = f"owner_id:{user_id} AND last_contact_agent_at:{rng}"

    tickets = {}
    for q in (q_created, q_touched):
        for t in search_tickets(q):
            tickets[t["id"]] = t

    ticket_cat = {}
    for tid, t in tickets.items():
        tags = zammad_get("/tags", {"object": "Ticket", "o_id": tid}).get("tags", [])
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

    cat_articles = {"listy": 0, "feedback": 0, "chat": 0, "dept": 0}
    notes = 0
    touches_total = 0
    for tid in tickets:
        arts = zammad_get(f"/ticket_articles/by_ticket/{tid}")
        for a in arts:
            if a.get("sender") != "Agent" or a.get("created_by") != login:
                continue
            ca = a.get("created_at", "")
            if not (f"{d0.isoformat()}T00:00:00" <= ca < f"{d1.isoformat()}T00:00:00"):
                continue
            touches_total += 1
            if a.get("type") == "note":
                notes += 1
            else:
                cat_articles[ticket_cat[tid]] += 1

    return {
        "listy": cat_articles["listy"],
        "feedback": cat_articles["feedback"],
        "chatz": cat_articles["chat"],
        "dept": cat_articles["dept"],
        "notes": notes,
        "touches": touches_total,
        "tickets": len(tickets),
    }


# ---------------------------------------------------------------------------
# Дешевий (тікет-рівневий) аналіз одного агента за день — для RANGE_DATA.
# ---------------------------------------------------------------------------

def range_agent_day(user_id, date_str):
    y, m, d = (int(x) for x in date_str.split("-"))
    d0 = date(y, m, d)
    d1 = d0 + timedelta(days=1)
    rng = f"[{d0.isoformat()}T00:00:00Z TO {d1.isoformat()}T00:00:00Z]"
    q = f"(created_by_id:{user_id} OR owner_id:{user_id}) AND created_at:{rng}"

    cats = {"listy": 0, "feedback": 0, "chatz": 0, "dept": 0}
    for t in search_tickets(q, limit=200):
        tags = zammad_get("/tags", {"object": "Ticket", "o_id": t["id"]}).get("tags", [])
        title = t.get("title") or ""
        gid = t.get("group_id")
        if "feedback_all" in tags:
            cats["feedback"] += 1
        elif title.startswith("Chat –") or title.startswith("Chat -"):
            cats["chatz"] += 1
        elif gid in DEPT_GROUPS:
            cats["dept"] += 1
        else:
            cats["listy"] += 1
    return cats


# ---------------------------------------------------------------------------
# Зміни з Supabase. Схема (portal_data: key="portal_v6", value=JSON з
# schedule/shiftTypes) підтверджена на реальному бекапі порталу.
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
        if isinstance(value, str):
            value = json.loads(value)
    except Exception:
        print("[shifts] не вдалось отримати розклад з Supabase:")
        traceback.print_exc()
        return None

    schedule = value.get("schedule", value) if isinstance(value, dict) else {}
    shift_types_raw = value.get("shiftTypes", {}) if isinstance(value, dict) else {}
    shift_types = {
        tid: {st["id"]: st for st in lst} for tid, lst in shift_types_raw.items()
    }
    if not shift_types.get(SCHEDULE_TEAM_ID):
        print(f"[shifts] у value нема shiftTypes для команди {SCHEDULE_TEAM_ID} — усі зміни підуть як OFF")
    return {"schedule": schedule, "shift_types": shift_types}


def shift_str_for(team_schedule, staff_id, date_str):
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
    time_ = st.get("time", "")
    return f"{label}@{time_}" if time_ else label


# ---------------------------------------------------------------------------
# Реальні чати з Simulator.company.
# ---------------------------------------------------------------------------

def fetch_chats_for_date(date_str):
    """{schedule_id: кількість чатів за день} по всій команді одним запитом,
    або {} якщо SIMULATOR_TOKEN не задано."""
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
# Кеш на диску (комітиться в git разом з index.html)
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
    range_cache = _load_json(RANGE_CACHE_PATH, {})

    precise_days = _date_range(yesterday, PRECISE_WINDOW_DAYS)
    range_days = _date_range(yesterday, RANGE_WINDOW_DAYS)

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

    for d in precise_days:
        if d in precise_cache:
            continue
        print(f"[refresh] тягну точні дані за {d}")
        day_result = {}
        for member in TEAM:
            zu = zammad_users.get(member["email"])
            if not zu:
                continue
            try:
                day_result[member["email"]] = analyze_agent_day(zu["id"], zu["login"], d)
            except Exception:
                print(f"[refresh] помилка analyze_agent_day {member['email']} {d}:")
                traceback.print_exc()
        precise_cache[d] = day_result
        _save_json(PRECISE_CACHE_PATH, precise_cache)

    for d in range_days:
        if d in range_cache:
            continue
        print(f"[refresh] тягну RANGE-дані за {d}")
        day_result = {}
        for member in TEAM:
            zu = zammad_users.get(member["email"])
            if not zu:
                continue
            try:
                day_result[str(member["id"])] = range_agent_day(zu["id"], d)
            except Exception:
                print(f"[refresh] помилка range_agent_day {member['email']} {d}:")
                traceback.print_exc()
        range_cache[d] = day_result
        _save_json(RANGE_CACHE_PATH, range_cache)

    _prune_cache(precise_cache, yesterday - timedelta(days=CACHE_KEEP_DAYS))
    _prune_cache(range_cache, yesterday - timedelta(days=CACHE_KEEP_DAYS))
    _save_json(PRECISE_CACHE_PATH, precise_cache)
    _save_json(RANGE_CACHE_PATH, range_cache)

    team_schedule = fetch_team_schedule()
    if team_schedule:
        shifts_by_day = {
            d: {m["email"].lower(): shift_str_for(team_schedule, m["id"], d) for m in TEAM}
            for d in precise_days
        }
        shifts_range = {
            d: {str(m["id"]): shift_str_for(team_schedule, m["id"], d) for m in TEAM}
            for d in range_days
        }
    else:
        shifts_by_day = {d: {} for d in precise_days}
        shifts_range = {d: {} for d in range_days}

    chats_cache = _load_json(CHATS_CACHE_PATH, {})
    if SIMULATOR_TOKEN:
        for d in range_days:
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

    build_html(precise_cache, range_cache, shifts_by_day, chats_cache, shifts_range, precise_days, range_days, yesterday)
    print(f"[refresh] готово {datetime.now(TZ).isoformat()}")


def replace_const(name, value_json, text):
    pattern = re.compile(r"const " + name + r" = .*?;", re.S)
    replacement = f"const {name} = {value_json};"
    new_text, n = pattern.subn(replacement, text, count=1)
    if n == 0:
        raise RuntimeError(f"не знайшла `const {name} = ...;` у шаблоні")
    return new_text


def build_html(precise_cache, range_cache, shifts_by_day, chats_cache, shifts_range, precise_days, range_days, yesterday):
    agents = []
    for member in TEAM:
        daily = []
        for d in precise_days:
            r = precise_cache.get(d, {}).get(member["email"], {})
            shift = shifts_by_day.get(d, {}).get(member["email"].lower(), "OFF")
            daily.append({
                "date": d,
                "touches": r.get("touches", 0),
                "tickets": r.get("tickets", 0),
                "shift": shift,
            })

        y = precise_cache.get(precise_days[-1], {}).get(member["email"], {})
        week_touches = sum(x["touches"] for x in daily)
        week_tickets = sum(x["tickets"] for x in daily)
        active_days = sum(1 for x in daily if x["touches"] > 0)
        yshift = shifts_by_day.get(precise_days[-1], {}).get(member["email"].lower(), "OFF")

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

    range_data = {}
    for d in range_days:
        day_chats = chats_cache.get(d, {})
        merged = {}
        for sid, cats in range_cache.get(d, {}).items():
            cats = dict(cats)
            if sid in day_chats:
                cats["chatz"] = day_chats[sid]
            merged[sid] = cats
        range_data[d] = merged

    range_min, range_max = range_days[0], range_days[-1]

    html = open(TEMPLATE_PATH, "r", encoding="utf-8").read()

    html = replace_const("AGENTS", json.dumps(agents, ensure_ascii=False, separators=(",", ":")), html)
    html = replace_const("RANGE_DATA", json.dumps(range_data, ensure_ascii=False, separators=(",", ":")), html)
    html = replace_const("RANGE_MIN", json.dumps(range_min), html)
    html = replace_const("RANGE_MAX", json.dumps(range_max), html)
    html = replace_const("SHIFTS_RANGE", json.dumps(shifts_range, ensure_ascii=False, separators=(",", ":")), html)

    now_str = datetime.now(TZ).strftime("%Y-%m-%d, %H:%M")
    html = re.sub(r"Оновлено: [^<]*\(Europe/Kiev\)", f"Оновлено: {now_str} (Europe/Kiev)", html)

    with open(OUTPUT_PATH, "w", encoding="utf-8") as f:
        f.write(html)


if __name__ == "__main__":
    regenerate()
