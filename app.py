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

# ---------------------------------------------------------------------------
# Simulator-звіти для вкладки "Simulator-звіти" (ринки / AVIA-теми / мови /
# тижнева таблиця) — ті самі дашборди, id/actorId стабільні (не змінюються
# від місяця до місяця, змінюється лише SIMULATOR_TOKEN, який згасає).
# Тягнуться НАЖИВО по запиту з фронтенду (/simulator-report), нічого з
# цього не кешується і не впливає на regenerate()/основний дашборд.
# ---------------------------------------------------------------------------
SIMREP_NAME_ID = "231d5660-f2c3-47fd-8b69-8eb44d720f17"  # спільний для ринків/AVIA

SIMREP_MARKETS_DASHBOARD = "cd59523d-b4b7-4e4e-87cf-496d68e619c6"
SIMREP_MARKETS_EXCLUDE = {"tickets.ua", "tickets.kz"}  # рішення користувача
SIMREP_MARKETS_LABELS = {
    "kissandfly.com": "COM", "travelfrom.es": "ES", "kissandfly.de": "DE",
    "mytickets.ae": "AE", "kissandfly.it": "IT", "travelfrom.fr": "FR",
    "alrehlat.com": "Aerhlat", "tickets.pl": "PL", "mytickets.co.il": "Co.il",
    "travelfrom.nl": "NL", "kissandfly.at": "AT", "kissandfly.ro": "RO",
    "tickets.kg": "KG", "tickets.lt": "LT", "tickets.uz": "UZ",
    "tickets.md": "MD", "tickets.az": "AZ", "tickets.ee": "EE",
    "tickets.lv": "LV", "tickets.ge": "GE", "tickets.am": "AM",
    "kissandfly.ng": "NG", "tickets.com.tr": "TR",
}
SIMREP_MARKETS_ACCOUNTS = [
    ("c3161d38-cdaf-4e44-a400-500a6a12d912", "tickets.ua"),
    ("5f48cbe3-d5e8-4123-8572-088e17c44fb8", "tickets.kz"),
    ("a87a2e24-e856-4c29-a41f-72daf6351e02", "travelfrom.es"),
    ("29dc34ca-1251-4271-878d-c8e03787015e", "travelfrom.fr"),
    ("979e43e9-b4cc-449a-9418-b3e0c6f98189", "travelfrom.nl"),
    ("92c722fe-0a68-4062-90d5-f83a4b5de0ea", "tickets.uz"),
    ("2f7e4ad4-c423-4303-b927-2a97a22cb9e9", "tickets.pl"),
    ("9f93b1a3-8382-45a2-8acd-d9ad8b327502", "alrehlat.com"),
    ("9bbb203f-442e-4656-a3b9-2fac6d3375e0", "mytickets.co.il"),
    ("3c8eabe9-1a02-4701-92a0-b7ebc11438ab", "kissandfly.ng"),
    ("ff28c4c8-895e-470a-acf1-fa4e280d47fb", "tickets.lv"),
    ("72c7e7f9-5343-4a3d-b8c1-90a4381393ae", "kissandfly.it"),
    ("419c1077-b677-4969-9ae4-c58951508cc3", "tickets.ee"),
    ("4a62f872-e5b7-4b95-9173-09a95176e98f", "tickets.az"),
    ("0990a776-b3b0-4fab-8c5f-8cb4f818e1d3", "tickets.am"),
    ("177c0b99-32fc-4190-ae47-d0e68297a44f", "tickets.com.tr"),
    ("d5111653-a5b3-4f2f-9855-bec0b036cac7", "kissandfly.ro"),
    ("f01dd7dc-a4b5-4b53-8d32-79e4ae67f4ea", "tickets.md"),
    ("06107230-32bc-4167-980d-bcf2b4741009", "tickets.lt"),
    ("fa43d533-8f14-4a1b-a1a7-e5666a813142", "tickets.kg"),
    ("9fad0eee-9892-416b-bece-de8e3206a80d", "tickets.ge"),
    ("68ca71a7-a3ec-4e95-ba69-b8c3f956d24e", "kissandfly.de"),
    ("de79e338-a1ff-4fc6-807c-3de0f2c2ffd4", "kissandfly.at"),
    ("150db01c-3cb4-4e16-9c50-fbbaeed96aef", "mytickets.ae"),
    ("c9f36b21-f354-411b-9250-84be84a09eea", "kissandfly.com"),
]

SIMREP_AVIA_DASHBOARD = "6044dc45-c6fd-4dca-b1dd-605349252dd9"
SIMREP_AVIA_ACCOUNTS = [
    ("1d8a933f-d825-487b-a72d-04b7e17d7962", "AVIA|Онлайн check-in 1001"),
    ("c1a0d59c-30c5-4ef8-9911-8cc33c41ebef", "AVIA|Терміни виписки квитка 1102"),
    ("1ae5f4c2-9862-4f66-b6c4-16d4d097ff87", "AVIA|Повернення інструкція/терміни 1003"),
    ("c5aa5deb-d577-4b12-a754-5eef514cf366", "AVIA|Інвол зміни/Очікування авторизації/відповіді АК/refund application 1007"),
    ("562961a2-8b7f-4507-bb1a-e85dcb1e257c", "AVIA|Date change/обмін загальна інформація (як подати запит/правила тарифу/терміни) 1106"),
    ("2c9cd215-ef4a-4351-bd71-66786edf28d1", "AVIA|Зміна данних пасажира 1002"),
    ("d28221b3-d5c2-454b-bed8-259dfe9f27ed", "AVIA|Статус \"Анульовано\" неуспішна виписка/розблокування коштів  1101"),
    ("407aa906-9e97-4ee5-83e7-f54997fa3405", "AVIA|Багаж оформити доп послугу 1206"),
    ("8a27c346-b60d-4646-ba12-bf9b71d7a92f", "AVIA|Багаж норми/вага/габарити 1205"),
    ("67fac00e-aaf9-41a1-99e5-69911c3fca2f", "AVIA|Загальна інформація по оплаті 1008"),
    ("67ea83d0-a35d-4dfe-8582-4cd71358655e", "AVIA|Повторна відправка МК 1103"),
    ("c8d68607-9c32-4244-9860-a0ba8e6069c8", "AVIA|Питають за повернення/затримка (заявка в жп) 1004"),
    ("12abb0b1-af56-44b4-81d7-7322da9c5200", "AVIA|Технічні труднощі при оплаті 1009"),
    ("08133bce-f0f4-41e2-a2b4-fe4217d6d7b8", "AVIA|Не коректний ПНР/не відкриває бронювання 1208"),
    ("6c705fe3-14b2-4087-b674-ac3ac12499f5", "AVIA|Авторизація повернення|RA 1005"),
    ("f7756dc4-7440-4d06-a471-bb906a2ae4ba", "AVIA|Місця оформити доп послугу 1109"),
    ("d6e7bb76-ca57-4785-b2ad-4d4d20f31f14", "AVIA|Відправка МК для підтвердження замовлення 1107"),
    ("af041edc-74c4-46ef-91d3-ea1d11ae579d", "AVIA|Документи для звітності/Інвойс 1204"),
    ("9f26850f-8234-4279-9b72-868cf11f5664", "AVIA|SSR запит, тварини, лижі, спорядження 1203"),
    ("338356a0-4e00-473d-a4b6-3dacfa389f64", "AVIA|Скарги/спірні питання/Чеки 1207"),
    ("6619825d-c96a-4228-8804-d766006369cd", "AVIA|Альта клієнту до виписки 1211"),
    ("dc4d83ed-9979-49a0-9daf-35b40156fd2b", "AVIA|АРН/Док про розблок 1209"),
    ("d391e7e0-eecd-4df3-9c4e-390e4f2c1835", "AVIA|Документи для здійснення подорожі/Візи 1202"),
    ("7b00ff54-992c-487e-bd7e-265f8f6c40c5", "AVIA|Не можемо оформити багаж 1213"),
    ("279d7a0d-0267-40a0-bd2c-f5b207cd3b42", "AVIA|Оновлений PNR 1214"),
    ("b07d32d3-760d-440f-b21f-f8e4942f25eb", "AVIA|Мильний/фродовий квиток (до вильоту) 1104"),
    ("24ad5dda-7589-4f1f-8fdd-988496f99057", "AVIA|Запит реквізитів 1006"),
    ("782ae41c-0db8-4e41-8a58-67db2724515d", "AVIA|Харчування/оформлення/наявність 1201"),
    ("6bdec1ca-7fe4-4b9e-8286-f70b9d2cc813", "AVIA|Виставлення рахунку для юр/фіз осіб 1212"),
    ("cf25b934-5c55-474b-9e2b-c60b33384dff", "AVIA|Зміна електронної адреси "),
]

SIMREP_LANG_DASHBOARD = "f63a133b-cf0b-4c43-9477-81773fefda25"
SIMREP_LANG_NAME_ID = "d8616859-65d1-4713-aedf-04d72c1856ab"
SIMREP_LANG_ACTOR_IDS = [
    "309a48d3-1ade-479b-984a-f72fec912b08", "bb377918-c3f8-4690-ac96-c06c75b7cdd5",
    "1ea39940-41cd-4455-a92a-aafd43ed87be", "808f866a-0086-465d-912f-da3de4120f62",
    "dafa4aaf-fdc9-441b-9278-8e77db1606d8", "a516574e-ad19-437b-8109-3c16c663a26e",
    "2b05e6a0-7a3f-4e71-a53e-aa291e8eed9c", "bdd1eacb-d740-48e1-970c-211314985f0c",
    "307f998c-e034-47f0-8cf1-a8c564f40c6e", "2269e4c1-c7f6-4b17-8d3c-fb1e6335ec81",
    "3b0f17c4-3304-4a4d-a2c5-2e546af952e6", "82b334ab-03d6-440e-9edf-5cb4b0a4fce7",
    "441c0ff0-270d-4ff3-a637-eea60c43585a", "2075c1d9-5f8b-4f25-9a33-ae647805d830",
    "891237f1-0b9e-495e-9a05-d71b1ee0bcf4", "a64001ef-e88d-4826-bb40-f65721ca08de",
    "0ea18f7e-06f7-4b52-b9b2-1c14c7c236a0", "3d5fa811-8f0c-408c-9381-eaf3b7846c1c",
    "d06f34a9-82da-4ce3-8f3f-973ae0b51e3f", "49859915-dab2-4543-877b-f79c3aae576b",
    "ca995139-4c9f-4d00-a329-f3e66ead2906", "05ec32cc-a5f9-410f-999c-c9b2bb6b9987",
    "ea9b4c4a-6266-4724-82c7-e6b0f56ed9ae", "3e8c5ba2-e4ef-4350-9d1b-0ffc018532d5",
    "f168f7ad-4aed-497f-bec1-1ef7e9b9f22e", "eeb3d0bd-08c2-43c0-afd2-ddfeff269eb7",
    "329ab56b-82f3-4535-bd7d-eacdd02f9275", "e9c798d6-a487-4d2d-b45c-525867978429",
    "904e66c3-26b3-4c8a-9693-f9bbd571c88d", "86b2f37a-eef2-4261-8a8c-209cbc1495f5",
    "85238a98-e09e-4c4f-b0b3-029e509b5872", "64986229-963d-428f-9fda-95576b7f6ec6",
    "4f1abd01-a9e9-490c-b2f0-5547d8b4afef", "4e4b1d11-e2d0-4ec7-b34e-451edc20b5a7",
    "3feb5275-acb5-4469-8bb8-8e4f4d66364b", "3e67f5b6-1de4-4352-ac68-f6a93e5ff919",
    "3375c753-cde3-44b6-8e5c-4ac583f6068d", "111286cf-93a9-456b-befd-48a8b8940c02",
    "a05984c3-9659-4572-846b-3ead14bc4c41",
]

# тижнева таблиця "Поступило/Відповіли/AI chats" — формула вивірена на
# серпні 2026: місячна сума Поступило (35679) й AI chats (19384) зійшлись
# з ручною табличкою користувача день-в-день з точністю ±3-9/день (межа
# доби на стику дат — на місячну суму не впливає).
SIMREP_WEEKLY_NAME_ID = "d0050cbd-1347-446d-90be-930ceee6431a"
SIMREP_INTERNATIONAL_TEAM_ID = "8f6fb408-242f-4067-b81f-ae199cd9196e"
SIMREP_WEEKLY_AI_GROUPS_DASHBOARD = "ce936289-c098-4e43-8141-53f1d1194f2e"
SIMREP_WEEKLY_AI_EMPLOYEES_DASHBOARD = "2efc1f46-2519-4de6-9f92-be2da2b8408c"
SIMREP_WEEKLY_MISSED_DASHBOARD = "b0a5bce2-d9d0-4010-a24c-5b5640bf74ac"
SIMREP_WEEKLY_MISSED_NAME_ID = "b12f1f93-fc5d-4033-99bc-4f35bd7e14ce"
SIMREP_WEEKLY_EMPLOYEES = [
    "d2bdeb5a-a769-49b6-a041-51509ac60d9e", "96f8f983-976d-4e6b-b551-157ab044abac",
    "caf82891-abdf-4078-9d97-7ff6f5bd6f29", "b5a17dd7-9aad-409c-9714-4a1f4e7e0d68",
    "46309104-eb31-4cf2-ba4d-3040c7e03010", "181ef978-1138-42ac-a874-da8f6590da0d",
    "fe72fd82-d62b-4074-826b-be146165a3c7", "24d54cb7-41d3-475c-af96-b93db683a055",
    "723d135b-c42f-4a8b-8aca-7e3769b3a013", "0f90d20a-8699-41e5-9084-e6a27b19e92b",
    "cb96c123-a267-4705-abae-cddabcb0a7b9", "51f99f07-4e40-4dc4-ac2b-51a7d3bd382e",
    "e905307e-483a-4a04-a8bd-6511ca707f2b", "7c6f0122-5d9a-4c9f-b46c-50c44b9cb9a3",
    "d04a30b7-5817-498c-b4db-6bf251bc656a", "47f015eb-252c-4af1-9d9c-740c2929e581",
    "3123465e-57bd-435b-81a0-8847e8496fb3",
]
SIMREP_DAYS_UA = ["понеділок", "вівторок", "середа", "четвер", "п'ятниця", "субота", "неділя"]

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
# Simulator-звіти для вкладки "Simulator-звіти" (ринки / AVIA-теми / мови /
# тижнева таблиця). Тягнуться НАЖИВО по HTTP GET /simulator-report — окремий
# період на кожен виклик, нічого не кешується і не чіпає ticket_cache/
# chats_cache основного дашборду.
# ---------------------------------------------------------------------------

def _simrep_params(from_date, to_date):
    d0 = datetime.strptime(from_date, "%Y-%m-%d").replace(tzinfo=TZ)
    d1 = datetime.strptime(to_date, "%Y-%m-%d").replace(hour=23, minute=59, second=59, tzinfo=TZ)
    return {
        "from": str(int(d0.timestamp() * 1000)),
        "to": str(int(d1.timestamp() * 1000)),
        "interval": "day",
        "timezoneOffset": "-180",
    }


def _simrep_post(dashboard_id, body, from_date, to_date):
    if not SIMULATOR_TOKEN:
        raise RuntimeError("SIMULATOR_TOKEN не задано")
    r = requests.post(
        f"{SIMULATOR_BASE}/api/1.0/dashboards/{dashboard_id}",
        params=_simrep_params(from_date, to_date),
        headers={"Authorization": f"Bearer {SIMULATOR_TOKEN}", "Content-Type": "application/json"},
        json=body,
        timeout=30,
    )
    r.raise_for_status()
    return r.json()["data"]


def _simrep_by_day(series):
    return {pt["date"][:10]: pt["value"] for pt in series.get("data", [])}


def fetch_markets_report(from_date, to_date):
    accounts = [
        {"actorId": aid, "account": None, "nameId": SIMREP_NAME_ID, "currencyId": SIMULATOR_CURRENCY_ID,
         "actor": {"id": aid, "title": title}, "incomeType": "total"}
        for aid, title in SIMREP_MARKETS_ACCOUNTS
    ]
    body = {"source": {"accounts": accounts, "counterType": "count", "chartType": "stackedBar",
                        "chartViewMode": "default"}}
    data = _simrep_post(SIMREP_MARKETS_DASHBOARD, body, from_date, to_date)

    rows = []
    for series in data:
        title = series["actorTitle"]
        if title in SIMREP_MARKETS_EXCLUDE:
            continue
        total = sum(pt["value"] for pt in series.get("data", []))
        rows.append((SIMREP_MARKETS_LABELS.get(title, title), total))
    rows.sort(key=lambda r: -r[1])
    return {"columns": ["Ринок", "Кількість"], "rows": [list(r) for r in rows], "total": sum(v for _, v in rows)}


def fetch_avia_report(from_date, to_date):
    accounts = [
        {"actorId": aid, "account": {}, "nameId": SIMREP_NAME_ID, "currencyId": SIMULATOR_CURRENCY_ID,
         "actor": {"id": aid, "title": title}, "accountType": "fact", "incomeType": "total"}
        for aid, title in SIMREP_AVIA_ACCOUNTS
    ]
    body = {"source": {"accounts": accounts, "counterType": "amount", "chartType": "bar"}}
    data = _simrep_post(SIMREP_AVIA_DASHBOARD, body, from_date, to_date)

    rows = sorted(((d["actorTitle"], d["value"]) for d in data), key=lambda x: -x[1])
    return {"columns": ["Тема", "Кількість"], "rows": [list(r) for r in rows], "total": sum(v for _, v in rows)}


def fetch_languages_report(from_date, to_date):
    accounts = [
        {"actorId": aid, "incomeType": "total", "currencyId": SIMULATOR_CURRENCY_ID, "nameId": SIMREP_LANG_NAME_ID}
        for aid in SIMREP_LANG_ACTOR_IDS
    ]
    body = {"source": {"accounts": accounts, "counterType": "amount", "chartType": "bar"}}
    data = _simrep_post(SIMREP_LANG_DASHBOARD, body, from_date, to_date)

    rows = sorted(((d.get("actorTitle", d["actorId"]), d["value"]) for d in data), key=lambda x: -x[1])
    return {"columns": ["Мова", "Кількість"], "rows": [list(r) for r in rows], "total": sum(v for _, v in rows)}


def fetch_weekly_table_report(from_date, to_date):
    body_groups = {"source": {"accounts": [
        {"actorId": SIMREP_INTERNATIONAL_TEAM_ID, "account": None, "nameId": SIMREP_WEEKLY_NAME_ID,
         "currencyId": SIMULATOR_CURRENCY_ID, "actor": {"id": SIMREP_INTERNATIONAL_TEAM_ID, "title": "International team"},
         "incomeType": "total"},
    ], "counterType": "amount", "chartType": "stackedBar", "chartViewMode": "default"}}
    data_groups = _simrep_post(SIMREP_WEEKLY_AI_GROUPS_DASHBOARD, body_groups, from_date, to_date)
    postuplilo = {}
    for series in data_groups:
        if series["actorTitle"] == "International team":
            postuplilo = _simrep_by_day(series)

    accounts_emp = [
        {"actorId": aid, "account": {}, "nameId": SIMREP_WEEKLY_NAME_ID, "currencyId": SIMULATOR_CURRENCY_ID,
         "actor": {"id": aid, "title": aid}, "accountType": "fact", "incomeType": "total"}
        for aid in SIMREP_WEEKLY_EMPLOYEES
    ]
    body_emp = {"source": {"accounts": accounts_emp, "counterType": "amount", "chartType": "stackedBar",
                            "chartViewMode": "default"}}
    data_emp = _simrep_post(SIMREP_WEEKLY_AI_EMPLOYEES_DASHBOARD, body_emp, from_date, to_date)
    escalated = {}
    for series in data_emp:
        for k, v in _simrep_by_day(series).items():
            escalated[k] = escalated.get(k, 0) + v

    body_missed = {"source": {"accounts": [
        {"actorId": SIMREP_INTERNATIONAL_TEAM_ID,
         "account": {"nameId": SIMREP_WEEKLY_MISSED_NAME_ID, "currencyId": 11294777, "accountName": "Missed chats",
                     "currencyName": "count", "currencyPrecision": 0, "currencyType": "number", "currencySymbol": ""},
         "nameId": SIMREP_WEEKLY_MISSED_NAME_ID, "currencyId": 11294777,
         "actor": {"id": SIMREP_INTERNATIONAL_TEAM_ID, "title": "International team"}, "incomeType": "total"},
    ], "counterType": "amount", "chartType": "stackedBar", "chartViewMode": "default"}}
    data_missed = _simrep_post(SIMREP_WEEKLY_MISSED_DASHBOARD, body_missed, from_date, to_date)
    missed = _simrep_by_day(data_missed[0]) if data_missed else {}

    # "Відповіли" — НЕ похідна (escalated-missed), а пряма сума чатів по
    # всіх агентах з того самого дашборду, що вже живить колонку "Чат" на
    # головній сторінці (SIMULATOR_DASHBOARD_ID/SIMULATOR_ACTORS) — так
    # хотіла користувачка, перевірено день-в-день з її ручною таблицею
    # (1.08: 424 — збіглось точно).
    accounts_answered = [
        {"actorId": aid, "account": {}, "nameId": SIMULATOR_NAME_ID, "currencyId": SIMULATOR_CURRENCY_ID,
         "actor": {"id": aid, "title": title}, "accountType": "fact", "incomeType": "total"}
        for aid, title in SIMULATOR_ACTORS.values()
    ]
    body_answered = {"source": {"accounts": accounts_answered, "counterType": "amount", "chartType": "stackedBar",
                                 "chartViewMode": "default"}}
    data_answered = _simrep_post(SIMULATOR_DASHBOARD_ID, body_answered, from_date, to_date)
    answered = {}
    for series in data_answered:
        for k, v in _simrep_by_day(series).items():
            answered[k] = answered.get(k, 0) + v

    d0 = date.fromisoformat(from_date)
    d1 = date.fromisoformat(to_date)
    days = [d0 + timedelta(days=i) for i in range((d1 - d0).days + 1)]

    rows = []
    tot_p = tot_m = tot_ai = tot_v = 0
    for d in days:
        key = d.isoformat()
        p = postuplilo.get(key, 0)
        m = missed.get(key, 0)
        e = escalated.get(key, 0)
        ai = p - e
        v = answered.get(key, 0)
        pct_ne = (m / p) if p else 0
        pct_v = 1 - pct_ne
        pct_ai = (ai / p) if p else 0
        tot_p += p; tot_m += m; tot_ai += ai; tot_v += v
        rows.append([
            d.strftime("%d.%m.%y"), SIMREP_DAYS_UA[d.weekday()],
            f"{pct_v*100:.2f}%", f"{pct_ne*100:.2f}%", p, v, m, ai, f"{pct_ai*100:.2f}%",
        ])

    pct_ne_tot = (tot_m / tot_p) if tot_p else 0
    pct_ai_tot = (tot_ai / tot_p) if tot_p else 0
    rows.append([
        "Разом", "", f"{(1-pct_ne_tot)*100:.2f}%", f"{pct_ne_tot*100:.2f}%",
        tot_p, tot_v, tot_m, tot_ai, f"{pct_ai_tot*100:.2f}%",
    ])

    return {
        "columns": ["Дата", "День", "% Відповіли", "% Не відповіли", "Поступило", "Відповіли", "Не відповіли",
                    "AI chats", "Трафік забраний AI"],
        "rows": rows,
        "total": tot_p,
    }


SIMREP_FETCHERS = {
    "markets": fetch_markets_report,
    "avia": fetch_avia_report,
    "languages": fetch_languages_report,
    "weekly": fetch_weekly_table_report,
}


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
        parsed = urlparse(self.path)
        if parsed.path == "/health":
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"ok")
            return
        if parsed.path == "/simulator-report":
            self._simulator_report(parsed)
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

    def _json(self, status, payload):
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(json.dumps(payload, ensure_ascii=False).encode())

    def _simulator_report(self, parsed):
        # GET /simulator-report?kind=markets|avia|languages|weekly&from=YYYY-MM-DD&to=YYYY-MM-DD
        # Наживо, без кешу — окрема вкладка "Simulator-звіти", не чіпає
        # regenerate()/основний дашборд.
        qs = parse_qs(parsed.query)
        kind = qs.get("kind", [None])[0]
        frm = qs.get("from", [None])[0]
        to = qs.get("to", [None])[0]
        if not kind or not frm or not to:
            self._json(400, {"error": "потрібно kind, from, to (YYYY-MM-DD)"})
            return
        fn = SIMREP_FETCHERS.get(kind)
        if not fn:
            self._json(400, {"error": f"невідомий kind: {kind}"})
            return
        if not SIMULATOR_TOKEN:
            self._json(503, {"error": "SIMULATOR_TOKEN не задано на сервері"})
            return
        try:
            result = fn(frm, to)
        except requests.HTTPError as e:
            self._json(502, {"error": f"Simulator API помилка (можливо, протух SIMULATOR_TOKEN — онови в Rancher secret): {e}"})
            return
        except Exception:
            traceback.print_exc()
            self._json(500, {"error": "внутрішня помилка сервера, дивись логи"})
            return
        self._json(200, result)

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
