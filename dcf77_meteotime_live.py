from __future__ import annotations
#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import json
import os
import tempfile
import threading
import time
from dataclasses import dataclass
from datetime import date, datetime
from typing import List, Optional

from gpiozero import DigitalInputDevice

ENABLE_SECTION7_OVERRIDE = False

# ============================================================
# GPIO / DCF77 input
# ============================================================

PIN = 17

# Used by many modules but may be to be changed:
# - OUT is inverted
# - Pull-up is needed
dcf = DigitalInputDevice(PIN, pull_up=True)


# ============================================================
# Web status output (non-intrusive side channel)
# ============================================================

STATUS_JSON_PATH = os.environ.get('DCF77_STATUS_JSON', '/tmp/dcf77_status.json')
STATUS_WRITE_INTERVAL_S = 1.0

WEEKDAY_NAMES_DE = {
    1: 'Montag',
    2: 'Dienstag',
    3: 'Mittwoch',
    4: 'Donnerstag',
    5: 'Freitag',
    6: 'Samstag',
    7: 'Sonntag',
}

TZ_NAMES_DE = {
    'CET': 'MEZ',
    'CEST': 'MESZ',
    'unknown': 'unbekannt',
}


def iso_now_local() -> str:
    return datetime.now().astimezone().isoformat(timespec='seconds')


def atomic_write_json(path: str, payload: dict):
    directory = os.path.dirname(path) or '.'
    os.makedirs(directory, exist_ok=True)
    with tempfile.NamedTemporaryFile('w', delete=False, dir=directory, encoding='utf-8') as tmp:
        json.dump(payload, tmp, ensure_ascii=False, indent=2)
        tmp.write('\n')
        tmp_path = tmp.name
    os.replace(tmp_path, path)


def create_initial_web_state(status_path: str) -> dict:
    return {
        'meta': {
            'status_path': status_path,
            'write_interval_s': STATUS_WRITE_INTERVAL_S,
            'started_at': iso_now_local(),
            'updated_at': iso_now_local(),
        },
        'rf': {
            'signal_state': int(dcf.value),
            'pulse_ms': None,
            'pause_ms': None,
            'period_ms': None,
            'bit': None,
            'bit_count': 0,
            'second': None,
            'current_bits': '',
            'current_triplet_state': '',
            'last_event': 'startup',
            'last_event_at': iso_now_local(),
            'minute_marker_detected': False,
            'last_minute_marker_at': None,
            'last_unusual_pulse_ms': None,
        },
        'dcf': {
            'valid': False,
            'decoded_at': None,
            'datetime_text': None,
            'date_text': None,
            'time_text': None,
            'weekday_index': None,
            'weekday_name': None,
            'timezone': None,
            'timezone_name_de': None,
            'sync_ok': False,
            'reason': 'waiting for first valid minute',
            'raw_bits': '',
        },
        'meteotime': {
            'available': False,
            'updated_at': None,
            'kind': 'idle',
            'summary': 'waiting for complete triplet',
            'triplet': None,
            'part': None,
            'minute_raw': None,
            'minute_minus_1': None,
            'weather_state': '',
            'reason': None,
            'decrypt_check': None,
            'cipher_hex': None,
            'key_hex': None,
            'plain_hex': None,
            'mapped': None,
            'last_good_mapped': None,
            'last_good_updated_at': None,
        },
        'stats': {},
    }


def decoded_minute_to_status(decoded: 'DecodedMinute') -> dict:
    return {
        'valid': True,
        'decoded_at': iso_now_local(),
        'datetime_text': f'{decoded.dd:02d}.{decoded.mo:02d}.{decoded.yy:04d} {decoded.hh:02d}:{decoded.mm:02d}',
        'date_text': f'{decoded.dd:02d}.{decoded.mo:02d}.{decoded.yy:04d}',
        'time_text': f'{decoded.hh:02d}:{decoded.mm:02d}',
        'weekday_index': decoded.weekday,
        'weekday_name': WEEKDAY_NAMES_DE.get(decoded.weekday, str(decoded.weekday)),
        'timezone': decoded.timezone,
        'timezone_name_de': TZ_NAMES_DE.get(decoded.timezone, decoded.timezone),
        'sync_ok': True,
        'reason': None,
        'raw_bits': ''.join(str(x) for x in decoded.bits59),
    }


def meteotime_result_to_status(
    result: dict,
    previous_last_good_mapped=None,
    previous_last_good_updated_at=None
) -> dict:
    status = {
        'updated_at': iso_now_local(),
        'kind': result['kind'],
        'available': False,
        'summary': None,
        'triplet': None,
        'part': None,
        'minute_raw': None,
        'minute_minus_1': None,
        'weather_state': None,
        'reason': None,
        'decrypt_check': None,
        'cipher_hex': None,
        'key_hex': None,
        'plain_hex': None,
        'mapped': None,
        'last_good_mapped': previous_last_good_mapped,
        'last_good_updated_at': previous_last_good_updated_at,
    }

    debug = result.get('debug')
    if debug is not None:
        status['triplet'] = debug.get('triplet')
        status['part'] = debug.get('part')
        status['minute_raw'] = debug.get('minute_raw')
        status['minute_minus_1'] = debug.get('minute_minus_1')
        status['weather_state'] = debug.get('weather_state')

    if result['kind'] == 'skip_invalid':
        status['summary'] = 'skipped before Meteotime triplet assembly'
        status['reason'] = result.get('reason')
        return status

    if result['kind'] == 'part0':
        status['summary'] = 'triplet part 0 stored; waiting for more'
        return status

    if result['kind'] == 'part1':
        status['summary'] = 'triplet part 1 stored; waiting for final part'
        return status

    if 'check' in result:
        status['decrypt_check'] = f"0x{result['check']:04X}"
    if 'cipher' in result:
        status['cipher_hex'] = ' '.join(f'{x:02X}' for x in result['cipher'])
    if 'key' in result:
        status['key_hex'] = ' '.join(f'{x:02X}' for x in result['key'])
    if 'plain' in result:
        status['plain_hex'] = ' '.join(f'{x:02X}' for x in result['plain'])

    if result['kind'] == 'decrypt_fail':
        status['summary'] = 'decrypt failed'
        status['reason'] = 'check != 0x2501'
        return status

    if result['kind'] == 'decrypt_ok':
        mapped = dict(result['mapped'])
        status['available'] = True
        status['summary'] = f"{mapped['region_name']} - {mapped['day_label']} / {mapped['section_kind']}"
        status['mapped'] = mapped
        status['last_good_mapped'] = mapped
        status['last_good_updated_at'] = status['updated_at']
        return status

    status['summary'] = result['kind']
    return status


def update_shared_state(shared_state: dict, lock: threading.Lock, updater):
    with lock:
        updater(shared_state)
        shared_state['meta']['updated_at'] = iso_now_local()


def status_writer_loop(shared_state: dict, lock: threading.Lock, status_path: str, stop_event: threading.Event):
    while not stop_event.is_set():
        with lock:
            snapshot = json.loads(json.dumps(shared_state, ensure_ascii=False))
        try:
            atomic_write_json(status_path, snapshot)
        except Exception as exc:
            print(f'STATUS   : write failed: {exc}')
        stop_event.wait(STATUS_WRITE_INTERVAL_S)


# ============================================================
# Meteotime tables
# ============================================================

mUintArrBitPattern12 = [0x80000, 0x00010, 0x00008, 0x00100, 0x00080, 0x01000, 0x00800, 0x10000, 0x08000, 0x00001, 0x00000, 0x00000]
mUintArrBitPattern30_1 = [
    0x00000200, 0x00000020, 0x02000000, 0x00000000, 0x00000000, 0x00000080, 0x40000000, 0x01000000,
    0x04000000, 0x00000000, 0x00010000, 0x00000000, 0x00400000, 0x00000010, 0x00200000, 0x00080000,
    0x00004000, 0x00000000, 0x00020000, 0x00100000, 0x00008000, 0x00000040, 0x00001000, 0x00000400,
    0x00000001, 0x80000000, 0x00000008, 0x00000002, 0x00040000, 0x10000000
]
mUintArrBitPattern30_2 = [
    0x00, 0x00, 0x00, 0x08, 0x20, 0x00, 0x00, 0x00,
    0x00, 0x10, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x40, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00
]
mUintArrBitPattern20 = [
    0x000004, 0x002000, 0x008000, 0x400000, 0x000100, 0x100000, 0x000400, 0x800000,
    0x040000, 0x020000, 0x000008, 0x000200, 0x004000, 0x000002, 0x001000, 0x080000,
    0x000800, 0x200000, 0x010000, 0x000001
]
mByteArrLookupTable1C_1 = [
    0xBB, 0x0E, 0x22, 0xC5, 0x73, 0xDF, 0xF7, 0x6D, 0x90, 0xE9, 0xA1, 0x38, 0x1C, 0x84, 0x4A, 0x56,
    0x64, 0x8D, 0x28, 0x0B, 0xD1, 0xBA, 0x93, 0x52, 0x1C, 0xC5, 0xA7, 0xF0, 0xE9, 0x7F, 0x36, 0x4E,
    0xC1, 0x77, 0x3D, 0xB3, 0xAA, 0xE0, 0x0C, 0x6F, 0x14, 0x88, 0xF6, 0x2B, 0xD2, 0x99, 0x5E, 0x45,
    0x1F, 0x70, 0x96, 0xD3, 0xB3, 0x0B, 0xFC, 0xEE, 0x81, 0x42, 0xCA, 0x34, 0xA5, 0x58, 0x29, 0x67
]
mByteArrLookupTable1C_2 = [
    0xAB, 0x3D, 0xFC, 0x74, 0x65, 0xE6, 0x0E, 0x4F, 0x97, 0x11, 0xD8, 0x59, 0x83, 0xC2, 0xBA, 0x20,
    0xC5, 0x1B, 0xD2, 0x58, 0x49, 0x37, 0x01, 0x7D, 0x93, 0xFA, 0xE0, 0x2F, 0x66, 0xB4, 0xAC, 0x8E,
    0xB7, 0xCC, 0x43, 0xFF, 0x58, 0x66, 0xEB, 0x35, 0x82, 0x2A, 0x99, 0xDD, 0x00, 0x71, 0x14, 0xAE,
    0x4E, 0xB1, 0xF7, 0x70, 0x18, 0x52, 0xAA, 0x9F, 0xD5, 0x6B, 0xCC, 0x3D, 0x04, 0x83, 0xE9, 0x26
]
mByteArrLookupTable1C_3 = [
    0x0A, 0x02, 0x00, 0x0F, 0x06, 0x07, 0x0D, 0x08, 0x03, 0x0C, 0x0B, 0x05, 0x09, 0x01, 0x04, 0x0E,
    0x02, 0x09, 0x05, 0x0D, 0x0C, 0x0E, 0x0F, 0x08, 0x06, 0x07, 0x0B, 0x01, 0x00, 0x0A, 0x04, 0x03,
    0x08, 0x00, 0x0D, 0x0F, 0x01, 0x0C, 0x03, 0x06, 0x0B, 0x04, 0x09, 0x05, 0x0A, 0x07, 0x02, 0x0E,
    0x03, 0x0D, 0x00, 0x0C, 0x09, 0x06, 0x0F, 0x0B, 0x01, 0x0E, 0x08, 0x0A, 0x02, 0x07, 0x04, 0x05
]
# These are the weather codes from the document DB W-Protokoll-V 1.pdf (DB W-Protokoll-V1.0.doc).
# These codes seem to interpret data often as "Frontengewitter" or "Nebel".
#WEATHER_CODES_DAY = {
#    0: "--", 1: "Sonnig", 2: "Leicht bewölkt", 3: "Vorwiegend bewölkt",
#    4: "Bedeckt", 5: "Hochnebel", 6: "Nebel", 7: "Regenschauer",
#    8: "Leichter Regen", 9: "Starker Regen", 10: "Frontengewitter",
#    11: "Wärmegewitter", 12: "Schneeregenschauer", 13: "Schneeschauer",
#    14: "Schneeregen", 15: "Schneefall",
#}

#WEATHER_CODES_NIGHT = {
#    0: "--", 1: "Klar", 2: "Leicht bewölkt", 3: "Vorwiegend bewölkt",
#    4: "Bedeckt", 5: "Hochnebel", 6: "Nebel", 7: "Regenschauer",
#    8: "Leichter Regen", 9: "Starker Regen", 10: "Frontengewitter",
#    11: "Wärmegewitter", 12: "Schneeregenschauer", 13: "Schneeschauer",
#    14: "Schneeregen", 15: "Schneefall",
#}

# These are the weather codes from the public available implementations.
# They seem to interpret data more plausible, also compared to a hardware DCF77 weather clock.
WEATHER_CODES_DAY = {
    0: "--",
    1: "Sonnig",
    2: "Leicht bewölkt",
    3: "Vorwiegend bewölkt",
    4: "Bedeckt",
    5: "Wärmegewitter",
    6: "Starker Regen",
    7: "Schnee",
    8: "Nebel",
    9: "Schneeregen",
    10: "Regenschauer",
    11: "Leichter Regen",
    12: "Schneeschauer",
    13: "Frontengewitter",
    14: "Hochnebel",
    15: "Schneefall",
}

WEATHER_CODES_NIGHT = {
    0: "--",
    1: "Klar",
    2: "Leicht bewölkt",
    3: "Vorwiegend bewölkt",
    4: "Bedeckt",
    5: "Wärmegewitter",
    6: "Starker Regen",
    7: "Schnee",
    8: "Nebel",
    9: "Schneeregen",
    10: "Regenschauer",
    11: "Leichter Regen",
    12: "Schneeschauer",
    13: "Frontengewitter",
    14: "Hochnebel",
    15: "Schneefall",
}

EXTREME_CODES = {
    0: "Kein", 1: "Schweres Wetter 24 Std.", 2: "Schweres Wetter (Tag)",
    3: "Schweres Wetter (Nacht)", 4: "Sturm", 5: "Sturm (Tag)",
    6: "Sturm (Nacht)", 7: "Böen (Tag)", 8: "Böen (Nacht)",
    9: "Eisregen Vormittag", 10: "Eisregen Nachmittag",
    11: "Eisregen Nacht", 12: "Feinstaub", 13: "Ozon",
    14: "Radiation", 15: "Hochwasser",
}

ANOMALY_JUMP_CODES = {
    0: "gleiches Wetter",
    1: "Sprung 1",
    2: "Sprung 2",
    3: "Sprung 3",
}

SUNSHINE_DURATION_CODES = {
    0: "0 - 2 Std.",
    1: "2 - 4 Std.",
    2: "5 - 6 Std.",
    3: "7 - 8 Std.",
}

WIND_DIRECTION_CODES = {
    0: "reserviert",
    1: "reserviert",
    2: "reserviert",
    3: "reserviert",
    4: "reserviert",
    5: "reserviert",
    6: "reserviert",
    7: "reserviert",
    8: "reserviert",
    9: "reserviert",
    10: "reserviert",
    11: "reserviert",
    12: "reserviert",
    13: "reserviert",
    14: "reserviert",
    15: "reserviert",
    16: "N",
    17: "NO",
    18: "O",
    19: "SO",
    20: "S",
    21: "SW",
    22: "W",
    23: "NW",
    24: "wechselnd",
    25: "Fön",
    26: "Bise NO",
    27: "Mistral N",
    28: "Scirocco S",
    29: "Tramont. W",
    30: "reserviert",
    31: "reserviert",
    32: "N",
    33: "NO",
    34: "O",
    35: "SO",
    36: "S",
    37: "SW",
    38: "W",
    39: "NW",
    40: "wechselnd",
    41: "Fön",
    42: "Bise NO",
    43: "Mistral N",
    44: "Scirocco S",
    45: "Tramont. W",
    46: "reserviert",
    47: "reserviert",
    48: "N",
    49: "NO",
    50: "O",
    51: "SO",
    52: "S",
    53: "SW",
    54: "W",
    55: "NW",
    56: "wechselnd",
    57: "Fön",
    58: "Bise NO",
    59: "Mistral N",
    60: "Scirocco S",
    61: "Tramont. W",
    62: "reserviert",
    63: "reserviert",
    64: "N",
    65: "NO",
    66: "O",
    67: "SO",
    68: "S",
    69: "SW",
    70: "W",
    71: "NW",
    72: "wechselnd",
    73: "Fön",
    74: "Bise NO",
    75: "Mistral N",
    76: "Scirocco S",
    77: "Tramont. W",
    78: "reserviert",
    79: "reserviert",
    80: "N",
    81: "NO",
    82: "O",
    83: "SO",
    84: "S",
    85: "SW",
    86: "W",
    87: "NW",
    88: "wechselnd",
    89: "Fön",
    90: "Bise NO",
    91: "Mistral N",
    92: "Scirocco S",
    93: "Tramont. W",
    94: "reserviert",
    95: "reserviert",
    96: "N",
    97: "NO",
    98: "O",
    99: "SO",
    100: "S",
    101: "SW",
    102: "W",
    103: "NW",
    104: "wechselnd",
    105: "Fön",
    106: "Bise NO",
    107: "Mistral N",
    108: "Scirocco S",
    109: "Tramont. W",
    110: "reserviert",
    111: "reserviert",
    112: "N",
    113: "NO",
    114: "O",
    115: "SO",
    116: "S",
    117: "SW",
    118: "W",
    119: "NW",
    120: "wechselnd",
    121: "Fön",
    122: "Bise NO",
    123: "Mistral N",
    124: "Scirocco S",
    125: "Tramont. W",
    126: "reserviert",
    127: "reserviert",
}

WIND_FORCE = {
    0: "0",
    1: "0-2",
    2: "3-4",
    3: "5-6",
    4: "7",
    5: "8",
    6: "9",
    7: ">=10",
}

REGIONS_ALL = {
    0: "Bordeaux / Südwestfrankreich",
    1: "La Rochelle / Westküste Frankreich",
    2: "Paris / Pariser Becken",
    3: "Brest / Bretagne",
    4: "Clermont-Ferrand / Zentralmassiv",
    5: "Béziers / Languedoc-Roussillon",
    6: "Bruxelles / Benelux",
    7: "Dijon / Ostfrankreich (Burgund)",
    8: "Marseille / Südfrankreich",
    9: "Lyon / Rhonetal",
    10: "Grenoble / Französische Alpen",
    11: "La Chaux-de-Fonds / Jura",
    12: "Frankfurt am Main / Unterer Rheingraben",
    13: "Westl. Mittelgebirge / Westliches Mittelgebirge",
    14: "Duisburg / Nordrhein-Westfalen",
    15: "Swansea / Westl. England & Wales",
    16: "Manchester / Nördliches England",
    17: "Le Havre / Normandie",
    18: "London / Südostengland",
    19: "Bremerhaven / Nordseeküste",
    20: "Herning / Nordwestliches Jütland",
    21: "Århus / Östliches Jütland",
    22: "Hannover / Norddeutschland",
    23: "København / Seeland",
    24: "Rostock / Ostseeküste",
    25: "Ingolstadt / Donautal",
    26: "München / Südbayern",
    27: "Bolzano / Südtirol",
    28: "Nürnberg / Nordbayern",
    29: "Leipzig / Sachsen",
    30: "Erfurt / Thüringen",
    31: "Lausanne / Westliches Schweizer Mittelland",
    32: "Zürich / Östliches Schweizer Mittelland",
    33: "Adelboden / Westlicher Schweizer Alpennordhang",
    34: "Sion / Wallis",
    35: "Glarus / Östlicher Schweizer Alpennordhang",
    36: "Davos / Graubünden",
    37: "Kassel / Mittelgebirge Ost",
    38: "Locarno / Tessin",
    39: "Sestriere / Piemont Alpen",
    40: "Milano / Poebene",
    41: "Roma / Toskana",
    42: "Amsterdam / Holland",
    43: "Génova / Golf von Genua",
    44: "Venezia / Pomündung",
    45: "Strasbourg / Oberer Rheingraben",
    46: "Klagenfurt / Österreichischer Alpensüdhang",
    47: "Innsbruck / Inneralpine Gebiete Österreich",
    48: "Salzburg / Alpennordhang Bayern/Österreich",
    49: "Bratislava / Wien-Region (AT/SK)",
    50: "Praha / Tschechisches Becken",
    51: "Decin / Erzgebirge",
    52: "Berlin / Ostdeutschland",
    53: "Göteborg / Westküste Schweden",
    54: "Stockholm / Stockholm-Region",
    55: "Kalmar / Schwedische Ostseeküste",
    56: "Jönköping / Südschweden",
    57: "Donaueschingen / Schwarzwald & Schwäbische Alb",
    58: "Oslo / Oslo-Region",
    59: "Stuttgart / Nördliches Baden-Württemberg",
    60: "Napoli", 61: "Ancona", 62: "Bari", 63: "Budapest", 64: "Madrid",
    65: "Bilbao", 66: "Palermo", 67: "Palma de Mallorca", 68: "Valencia", 69: "Barcelona",
    70: "Andorra", 71: "Sevilla", 72: "Lissabon", 73: "Sassari", 74: "Gijon",
    75: "Galway", 76: "Dublin", 77: "Glasgow", 78: "Stavanger", 79: "Trondheim",
    80: "Sundsvall", 81: "Gdansk", 82: "Warszawa", 83: "Krakow", 84: "Umea",
    85: "Oestersund", 86: "Samedan", 87: "Zagreb", 88: "Zermatt", 89: "Split",
}

SECTION_INFO = {
    0: ("Heute", "Tag-Block", "12h Tag + 24h Schweres Wetter/Regen"),
    1: ("Heute", "Nacht-Block", "12h Nacht + 24h Wind"),
    2: ("Tag 1", "Tag-Block", "12h Tag + 24h Schweres Wetter/Regen"),
    3: ("Tag 1", "Nacht-Block", "12h Nacht + 24h Wind"),
    4: ("Tag 2", "Tag-Block", "12h Tag + 24h Schweres Wetter/Regen"),
    5: ("Tag 2", "Nacht-Block", "12h Nacht + 24h Wind"),
    6: ("Tag 3", "Tag-Block", "12h Tag + 24h Schweres Wetter/Regen"),
    7: ("Tag 3", "Tag-Block", "12h Tag + 24h Schweres Wetter/Regen"),
}


# ============================================================
# Data classes
# ============================================================

@dataclass
class Row:
    weather: str
    info: str
    minutebits: str
    hourbits: str
    daybits: str
    wotbits: str
    monthbits: str
    yearbits: str
    dd: int
    mo: int
    yy: int
    hh: int
    mm: int
    ss: int


@dataclass
class DecodedMinute:
    bits59: List[int]
    dd: int
    mo: int
    yy: int
    hh: int
    mm: int
    ss: int
    weekday: int
    timezone: str

    def to_row(self) -> Row:
        b = self.bits59

        weather = ''.join(str(x) for x in b[1:15])
        info = ''.join(str(x) for x in b[15:21])
        minutebits = ''.join(str(x) for x in b[21:29])
        hourbits = ''.join(str(x) for x in b[29:36])
        daybits = ''.join(str(x) for x in b[36:42])
        wotbits = ''.join(str(x) for x in b[42:45])
        monthbits = ''.join(str(x) for x in b[45:50])
        yearbits = ''.join(str(x) for x in b[50:59])

        return Row(
            weather=weather,
            info=info,
            minutebits=minutebits,
            hourbits=hourbits,
            daybits=daybits,
            wotbits=wotbits,
            monthbits=monthbits,
            yearbits=yearbits,
            dd=self.dd,
            mo=self.mo,
            yy=self.yy % 100,
            hh=self.hh,
            mm=self.mm,
            ss=self.ss,
        )


class ByteUInt:
    def __init__(self):
        self.FullUint = 0

    @property
    def Byte0(self):
        return self.FullUint & 0xFF

    @Byte0.setter
    def Byte0(self, v):
        self.FullUint = (self.FullUint & ~0xFF) | (v & 0xFF)

    @property
    def Byte1(self):
        return (self.FullUint >> 8) & 0xFF

    @Byte1.setter
    def Byte1(self, v):
        self.FullUint = (self.FullUint & ~(0xFF << 8)) | ((v & 0xFF) << 8)

    @property
    def Byte2(self):
        return (self.FullUint >> 16) & 0xFF

    @Byte2.setter
    def Byte2(self, v):
        self.FullUint = (self.FullUint & ~(0xFF << 16)) | ((v & 0xFF) << 16)

    @property
    def Byte3(self):
        return (self.FullUint >> 24) & 0xFF

    @Byte3.setter
    def Byte3(self, v):
        self.FullUint = (self.FullUint & ~(0xFF << 24)) | ((v & 0xFF) << 24)


class Container:
    def __init__(self):
        self.mByteUint1 = ByteUInt()
        self.mByteUint2 = ByteUInt()
        self.mByteUint3 = ByteUInt()
        self.mByteUint4 = ByteUInt()
        self.mByteUpperTime2 = 0
        self.mUintLowerTime = 0


# ============================================================
# DCF77 helpers
# ============================================================

def classify_pulse(ms: float) -> Optional[int]:
    if ms < 25 or ms > 280:
        return None
    return 0 if abs(ms - 100) <= abs(ms - 200) else 1


def parity_ok_block(bit_list):
    return (sum(bit_list) % 2) == 0


def bcd_value(bits_subset, weights):
    return sum(bit * weight for bit, weight in zip(bits_subset, weights))


def decode_time_minute(bits59: List[int]):
    if len(bits59) != 59:
        return None, f"expected 59 bits, got {len(bits59)}"

    start_bit = bits59[20]

    minute_bits = bits59[21:28]
    minute_parity_bit = bits59[28]

    hour_bits = bits59[29:35]
    hour_parity_bit = bits59[35]

    day_bits = bits59[36:42]
    dow_bits = bits59[42:45]
    month_bits = bits59[45:50]
    year_bits = bits59[50:58]
    date_parity_bit = bits59[58]

    minute = bcd_value(minute_bits, [1, 2, 4, 8, 10, 20, 40])
    hour = bcd_value(hour_bits, [1, 2, 4, 8, 10, 20])
    day = bcd_value(day_bits, [1, 2, 4, 8, 10, 20])
    dow = bcd_value(dow_bits, [1, 2, 4])
    month = bcd_value(month_bits, [1, 2, 4, 8, 10])
    year = bcd_value(year_bits, [1, 2, 4, 8, 10, 20, 40, 80])

    minute_parity_ok = parity_ok_block(minute_bits + [minute_parity_bit])
    hour_parity_ok = parity_ok_block(hour_bits + [hour_parity_bit])
    date_parity_ok = parity_ok_block(day_bits + dow_bits + month_bits + year_bits + [date_parity_bit])

    cest = bits59[17]
    cet = bits59[18]
    timezone = "unknown"
    if cet == 1 and cest == 0:
        timezone = "CET"
    elif cest == 1 and cet == 0:
        timezone = "CEST"

    reasons = []
    if start_bit != 1:
        reasons.append("start bit 20 != 1")
    if not minute_parity_ok:
        reasons.append("minute parity failed")
    if not hour_parity_ok:
        reasons.append("hour parity failed")
    if not date_parity_ok:
        reasons.append("date parity failed")
    if not (0 <= minute <= 59):
        reasons.append(f"minute invalid: {minute}")
    if not (0 <= hour <= 23):
        reasons.append(f"hour invalid: {hour}")
    if not (1 <= day <= 31):
        reasons.append(f"day invalid: {day}")
    if not (1 <= month <= 12):
        reasons.append(f"month invalid: {month}")
    if not (1 <= dow <= 7):
        reasons.append(f"weekday invalid: {dow}")

    if reasons:
        return None, "; ".join(reasons)

    decoded = DecodedMinute(
        bits59=bits59[:],
        dd=day,
        mo=month,
        yy=2000 + year,
        hh=hour,
        mm=minute,
        ss=0,
        weekday=dow,
        timezone=timezone,
    )
    return decoded, None


# ============================================================
# Reference decoder helper
# ============================================================

def parity_ok(a, s, e):
    return (sum(a[s:e]) & 1) == a[e]


def parse_message(r: Row):
    a = [0] * 60
    for i, ch in enumerate(r.weather, start=1):
        a[i] = 1 if ch == '1' else 0
    for i, ch in enumerate(r.info, start=15):
        a[i] = 1 if ch == '1' else 0
    for i, ch in enumerate(r.minutebits, start=21):
        a[i] = 1 if ch == '1' else 0
    for i, ch in enumerate(r.hourbits, start=29):
        a[i] = 1 if ch == '1' else 0
    for i, ch in enumerate(r.daybits, start=36):
        a[i] = 1 if ch == '1' else 0
    for i, ch in enumerate(r.wotbits, start=42):
        a[i] = 1 if ch == '1' else 0
    for i, ch in enumerate(r.monthbits, start=45):
        a[i] = 1 if ch == '1' else 0
    for i, ch in enumerate(r.yearbits, start=50):
        a[i] = 1 if ch == '1' else 0
    return a


def CopyTimeToByteUint(data, key, c: Container):
    c.mByteUint1.FullUint = c.mByteUint2.FullUint = c.mByteUint3.FullUint = 0
    c.mUintLowerTime = 0
    c.mByteUpperTime2 = 0
    for i in range(4):
        c.mUintLowerTime = ((c.mUintLowerTime << 8) | key[3 - i]) & 0xFFFFFFFF
    c.mByteUpperTime2 = key[4]
    c.mByteUint3.Byte0 = data[2]
    c.mByteUint3.Byte1 = data[3]
    c.mByteUint3.Byte2 = data[4]
    c.mByteUint3.FullUint >>= 4
    c.mByteUint2.Byte0 = data[0]
    c.mByteUint2.Byte1 = data[1]
    c.mByteUint2.Byte2 = data[2] & 0x0F


def ShiftTimeRight(round_, c: Container):
    count = 2 if round_ in (16, 8, 7, 3) else 1
    while count:
        count -= 1
        tmp = 1 if (c.mUintLowerTime & 0x00100000) else 0
        c.mUintLowerTime &= 0xFFEFFFFF
        if c.mUintLowerTime & 1:
            c.mUintLowerTime |= 0x00100000
        c.mUintLowerTime >>= 1
        if c.mByteUpperTime2 & 1:
            c.mUintLowerTime |= 0x80000000
        c.mByteUpperTime2 >>= 1
        if tmp:
            c.mByteUpperTime2 |= 0x80
        c.mUintLowerTime &= 0xFFFFFFFF
        c.mByteUpperTime2 &= 0xFF


def ExpandR(c: Container):
    c.mByteUint3.FullUint &= 0x000FFFFF
    tmp = 0x00100000
    for i in range(12):
        if c.mByteUint3.FullUint & mUintArrBitPattern12[i]:
            c.mByteUint3.FullUint |= tmp
        tmp <<= 1
    c.mByteUint3.FullUint &= 0xFFFFFFFF


def CompressKey(c: Container):
    tmp = 1
    c.mByteUint1.FullUint = 0
    for i in range(30):
        if (c.mUintLowerTime & mUintArrBitPattern30_1[i]) or (c.mByteUpperTime2 & mUintArrBitPattern30_2[i]):
            c.mByteUint1.FullUint |= tmp
        tmp <<= 1
    c.mByteUint1.FullUint &= 0xFFFFFFFF


def DoSbox(c: Container):
    helper = c.mByteUint1.Byte3
    c.mByteUint1.Byte3 = c.mByteUint1.Byte2
    c.mByteUint4.FullUint = 0
    for i in range(5, 0, -1):
        if (i & 1) == 0:
            tmp = (c.mByteUint1.Byte0 >> 4) | ((c.mByteUint1.Byte0 & 0x0F) << 4)
            c.mByteUint1.Byte0 = tmp
        c.mByteUint1.Byte3 &= 0xF0
        tmp = ((c.mByteUint1.Byte0 & 0x0F) | c.mByteUint1.Byte3) & 0xFF
        if i & 4:
            tmp = mByteArrLookupTable1C_1[tmp & 0x3F]
        if i & 2:
            tmp = mByteArrLookupTable1C_2[tmp & 0x3F]
        elif i == 1:
            tmp = mByteArrLookupTable1C_3[tmp & 0x3F]
        if i & 1:
            c.mByteUint4.Byte0 = tmp & 0x0F
        else:
            c.mByteUint4.Byte0 |= tmp & 0xF0
        if (i & 1) == 0:
            tmp2 = c.mByteUint1.Byte3
            c.mByteUint1.FullUint >>= 8
            c.mByteUint1.Byte3 = tmp2
            c.mByteUint4.FullUint = (c.mByteUint4.FullUint << 8) & 0xFFFFFFFF
        c.mByteUint1.Byte3 >>= 1
        if helper & 1:
            c.mByteUint1.Byte3 |= 0x80
        helper >>= 1
        c.mByteUint1.Byte3 >>= 1
        if helper & 1:
            c.mByteUint1.Byte3 |= 0x80
        helper >>= 1


def DoPbox(c: Container):
    tmp = 1
    c.mByteUint1.FullUint = 0xFF000000
    for i in range(20):
        if c.mByteUint4.FullUint & mUintArrBitPattern20[i]:
            c.mByteUint1.FullUint |= tmp
        tmp <<= 1
    c.mByteUint1.FullUint &= 0xFFFFFFFF


def decrypt(cipher, key):
    c = Container()
    CopyTimeToByteUint(cipher, key, c)
    for i in range(16, 0, -1):
        ShiftTimeRight(i, c)
        ExpandR(c)
        CompressKey(c)
        c.mByteUint1.FullUint ^= c.mByteUint3.FullUint
        c.mByteUint3.Byte2 &= 0x0F
        DoSbox(c)
        DoPbox(c)
        c.mByteUint1.FullUint ^= c.mByteUint2.FullUint
        c.mByteUint2.FullUint = c.mByteUint3.FullUint & 0x00FFFFFF
        c.mByteUint3.FullUint = c.mByteUint1.FullUint & 0x00FFFFFF
    c.mByteUint3.FullUint = (c.mByteUint3.FullUint << 4) & 0xFFFFFFFF
    c.mByteUint2.Byte2 &= 0x0F
    c.mByteUint2.Byte2 |= c.mByteUint3.Byte0 & 0xF0
    return [c.mByteUint2.Byte0, c.mByteUint2.Byte1, c.mByteUint2.Byte2, c.mByteUint3.Byte1, c.mByteUint3.Byte2]


def flip_byte(x: int) -> int:
    result = 0
    source = x
    for _ in range(8):
        result >>= 1
        result |= source & 0x80
        source = (source << 1) & 0xFF
    return result


def swab_nibble(value: int) -> int:
    out = 0
    for _ in range(4):
        out = (out << 1) | (value & 0x01)
        value >>= 1
    return out


def payload_to_info_bytes(payload: int):
    return list(payload.to_bytes(3, 'big'))


def decode_weather_info(payload: int):
    info = payload_to_info_bytes(payload)

    day_code = swab_nibble(info[0] >> 4)
    night_code = swab_nibble(info[0] & 0x0F)
    anomaly = info[1] & 0x01

    # Bits 8..11 are one mirrored 4-bit field.
    # If anomaly_bit == 0: extreme weather code
    # If anomaly_bit == 1: bits 8..9 = relative morning weather, bits 10..11 = sunshine duration
    bits8_11 = swab_nibble(info[1] >> 4)
    extreme_code = bits8_11
    morning_jump_code = bits8_11 & 0x03
    sunshine_code = (bits8_11 >> 2) & 0x03

    # Rain probability is a 3-bit field in bits 1..3 of info[1].
    # Extract the field cleanly first, then reverse the 3-bit order.
    rain_raw = (info[1] >> 1) & 0x07
    rain_group = ((rain_raw & 0x01) << 2) | (rain_raw & 0x02) | ((rain_raw & 0x04) >> 2)
    rain_percent = min(rain_group * 15, 100)

    temp_raw = info[2] >> 2
    temp_code = 0
    for _ in range(6):
        temp_code = (temp_code << 1) | (temp_raw & 0x01)
        temp_raw >>= 1

    if temp_code == 0:
        temp_text = '< -21 °C'
    elif temp_code == 63:
        temp_text = '> 40 °C'
    else:
        temp_text = f'{temp_code - 22} °C'

    if anomaly == 0:
        bits8_11_mode = 'extreme_weather'
        bits8_11_text = EXTREME_CODES.get(extreme_code, f'Code {extreme_code}')
    else:
        bits8_11_mode = 'weather_anomaly'
        bits8_11_text = (
            f"Relatives Vormittagswetter = {ANOMALY_JUMP_CODES.get(morning_jump_code, f'Code {morning_jump_code}')}, "
            f"Sonnenscheindauer = {SUNSHINE_DURATION_CODES.get(sunshine_code, f'Code {sunshine_code}')}"
        )


    return {
        'payload_hex': f'0x{payload:06X}',
        'info0_hex': f'{info[0]:02X}',
        'info1_hex': f'{info[1]:02X}',
        'info2_hex': f'{info[2]:02X}',
        'day_code': day_code,
        'day_weather': WEATHER_CODES_DAY.get(day_code, f'Code {day_code}'),
        'night_code': night_code,
        'night_weather': WEATHER_CODES_NIGHT.get(night_code, f'Code {night_code}'),
        'anomaly_bit': anomaly,
        'bits8_11_mode': bits8_11_mode,
        'bits8_11_raw_code': bits8_11,
        'bits8_11_text': bits8_11_text,
        'extreme_code': extreme_code,
        'extreme_text': EXTREME_CODES.get(extreme_code, f'Code {extreme_code}'),
        'morning_jump_code': morning_jump_code,
        'morning_jump_text': ANOMALY_JUMP_CODES.get(morning_jump_code, f'Code {morning_jump_code}'),
        'sunshine_code': sunshine_code,
        'sunshine_text': SUNSHINE_DURATION_CODES.get(sunshine_code, f'Code {sunshine_code}'),
        'rain_group': rain_group,
        'rain_percent': rain_percent,
        'wind_dir_code': extreme_code,
        'wind_force_code': rain_group,
        'wind_full_code': (rain_group << 4) | extreme_code,
        'wind_direction': WIND_DIRECTION_CODES.get((rain_group << 4) | extreme_code, f'Code {(rain_group << 4) | extreme_code}'),
        'wind_force': WIND_FORCE.get(rain_group, f'Code {rain_group}'),
        'wind_direction_valid_when_anomaly_bit_is_0': True,
        'temp_code': temp_code,
        'temp_text': temp_text,
    }


def last_sunday(year: int, month: int) -> int:
    d = date(year, month + 1, 1) if month < 12 else date(year + 1, 1, 1)
    d = d.fromordinal(d.toordinal() - 1)
    while d.weekday() != 6:
        d = d.fromordinal(d.toordinal() - 1)
    return d.day


def is_dst_europe_local(row: Row) -> bool:
    year = 2000 + row.yy
    if row.mo < 3 or row.mo > 10:
        return False
    if 3 < row.mo < 10:
        return True
    if row.mo == 3:
        ls = last_sunday(year, 3)
        if row.dd > ls:
            return True
        if row.dd < ls:
            return False
        return row.hh >= 2
    ls = last_sunday(year, 10)
    if row.dd < ls:
        return True
    if row.dd > ls:
        return False
    return row.hh < 3


def get_minutes_since_2200_utc_anchor(row: Row) -> int:
    hours = row.hh
    hours -= 1  # CET -> UTC
    if is_dst_europe_local(row):
        hours -= 1
    hours -= 22
    if hours < 0:
        hours += 24
    return row.mm + hours * 60


def get_area_section(row: Row):
    schedule_minutes = (get_minutes_since_2200_utc_anchor(row) - 1) % (24 * 60)

    if 0 <= schedule_minutes <= 179:
        area = schedule_minutes // 3
        section = 0
    elif 180 <= schedule_minutes <= 359:
        area = (schedule_minutes - 180) // 3
        section = 1
    elif 360 <= schedule_minutes <= 539:
        area = (schedule_minutes - 360) // 3
        section = 2
    elif 540 <= schedule_minutes <= 719:
        area = (schedule_minutes - 540) // 3
        section = 3
    elif 720 <= schedule_minutes <= 899:
        area = (schedule_minutes - 720) // 3
        section = 4
    elif 900 <= schedule_minutes <= 1079:
        area = (schedule_minutes - 900) // 3
        section = 5
    elif 1080 <= schedule_minutes <= 1169:
        area = (schedule_minutes - 1080) // 3
        section = 6
    elif 1170 <= schedule_minutes <= 1259:
        area = (schedule_minutes - 1170) // 3
        section = 7
    elif 1260 <= schedule_minutes <= 1349:
        area = 60 + ((schedule_minutes - 1260) // 3)
        section = 0
    else:
        area = 60 + ((schedule_minutes - 1350) // 3)
        section = 1

    return area, section


def get_region_meta(region_id: int):
    name = REGIONS_ALL.get(region_id, f'Region {region_id}')
    forecast_days = 4 if region_id <= 59 else 2
    return name, forecast_days


def add_region_section(mapped: dict, row: Row):
    area, section = get_area_section(row)
    region_name, forecast_days = get_region_meta(area)

    mapped['region_id'] = area
    mapped['region_name'] = region_name
    mapped['forecast_days'] = forecast_days
    mapped['section_id'] = section

    # Regions 60..89 use a reduced 2-day forecast model:
    # one 90-minute block for today and one 90-minute block for the following day.
    # Only day/night weather and one HI temperature are shown.
    if area >= 60:
        day_label = 'Heute' if section == 0 else 'Tag 1'
        section_kind = '2-Tages-Prognose'
        interpretation = 'nur Wetter Tag/Nacht + Temperatur (HI)'

        mapped['day_label'] = day_label
        mapped['section_kind'] = section_kind
        mapped['interpretation'] = interpretation
        mapped['is_high_section'] = False
        mapped['is_low_wind_section'] = False
        mapped['display_day_weather'] = mapped['day_weather']
        mapped['display_day_code'] = mapped['day_code']
        mapped['display_night_weather'] = mapped['night_weather']
        mapped['display_night_code'] = mapped['night_code']
        mapped['section_value_text'] = '-'
        mapped['temp_text'] = f"{mapped['temp_text']} (HI)"
        return mapped

    day_label, section_kind, interpretation = SECTION_INFO.get(section, (f'Sektion {section}', '?', '?'))
    mapped['day_label'] = day_label
    mapped['section_kind'] = section_kind
    mapped['interpretation'] = interpretation
    mapped['is_high_section'] = section in (0, 2, 4, 6, 7)
    mapped['is_low_wind_section'] = section in (1, 3, 5)
    if mapped['is_high_section']:
        mapped['display_day_weather'] = mapped['day_weather']
        mapped['display_day_code'] = mapped['day_code']
        mapped['display_night_weather'] = mapped['night_weather']
        mapped['display_night_code'] = mapped['night_code']

        # The Bit-15 anomaly interpretation for "Tag" is only valid for
        # section 0 (Heute / Hoch). For forecast days (sections 2/4/6),
        # Bit 15 is treated as not applicable and the block is always
        # interpreted as extreme weather + rain. This makes the decoder
        # more robust against occasional bit errors in later high sections.
        mapped['day_anomaly_mode_valid'] = (section == 0)

        if mapped['anomaly_bit'] == 1 and mapped['day_anomaly_mode_valid']:
            mapped['section_value_text'] = (
                f"Relatives Vormittagswetter = {mapped['morning_jump_text']} (Code {mapped['morning_jump_code']}), "
                f"Sonnenscheindauer = {mapped['sunshine_text']} (Code {mapped['sunshine_code']}), "
                f"Regen = {mapped['rain_percent']} %"
            )
        else:
            mapped['section_value_text'] = (
                f"Schweres Wetter = {mapped['extreme_text']} (Code {mapped['extreme_code']}), "
                f"Regen = {mapped['rain_percent']} %"
            )
    else:
        mapped['display_day_weather'] = mapped['day_weather']
        mapped['display_day_code'] = mapped['day_code']
        mapped['display_night_weather'] = mapped['night_weather']
        mapped['display_night_code'] = mapped['night_code']

        if mapped['anomaly_bit'] == 0:
            mapped['section_value_text'] = (
                f"Wind = {mapped['wind_direction']} (Code {mapped['wind_dir_code']}), "
                f"Stärke = {mapped['wind_force']} (Code {mapped['wind_force_code']})"
            )
        else:
            mapped['section_value_text'] = (
                f"Schweres Wetter = {mapped['extreme_text']} (Code {mapped['extreme_code']})"
            )
    return mapped


# ============================================================
# Meteotime state machine
# ============================================================

def format_weather_state(weather_state: List[int]) -> str:
    chunks = [
        ''.join(str(x) for x in weather_state[0:14]),
        ''.join(str(x) for x in weather_state[14:28]),
        ''.join(str(x) for x in weather_state[28:42]),
        ''.join(str(x) for x in weather_state[42:82]),
    ]
    return f"W1={chunks[0]} W2={chunks[1]} W3={chunks[2]} KEY={chunks[3]}"


def process_meteotime_minute(decoded: DecodedMinute, weather_state: List[int], stats: dict):
    r = decoded.to_row()
    a = parse_message(r)

    if not (a[20] == 1 and parity_ok(a, 21, 28) and parity_ok(a, 29, 35) and parity_ok(a, 36, 58)):
        stats['skipped_for_meteotime'] += 1
        return {
            'kind': 'skip_invalid',
            'reason': 'invalid DCF minute for Meteotime',
        }

    minute_raw = (a[21] + a[22] * 2 + a[23] * 4 + a[24] * 8) + 10 * (a[25] + a[26] * 2 + a[27] * 4)
    minute_minus_1 = (minute_raw - 1) & 0xFF
    part = minute_minus_1 % 3
    triplet = minute_minus_1 // 3

    debug = {
        'triplet': triplet,
        'part': part,
        'minute_raw': minute_raw,
        'minute_minus_1': minute_minus_1,
    }

    if part == 0:
        stats['part0'] += 1
        stats['triplets_started'] += 1

        for i in range(82):
            weather_state[i] = 0
        for i in range(14):
            weather_state[i] = a[i + 1]

        debug['action'] = 'stored first 14 weather bits; reset weather_state'
        debug['weather_state'] = format_weather_state(weather_state)
        return {'kind': 'part0', 'debug': debug}

    if part == 1:
        stats['part1'] += 1

        for i in range(14):
            weather_state[14 + i] = a[i + 1]

        j = 42
        for i in range(21, 28):
            weather_state[j] = a[i]
            j += 1
        j += 1
        for i in range(29, 35):
            weather_state[j] = a[i]
            j += 1
        j += 2
        for i in range(36, 42):
            weather_state[j] = a[i]
            j += 1
        j += 2
        for i in range(45, 50):
            weather_state[j] = a[i]
            j += 1
        for i in range(42, 45):
            weather_state[j] = a[i]
            j += 1
        for i in range(50, 58):
            weather_state[j] = a[i]
            j += 1

        debug['action'] = 'stored second 14 weather bits + assembled key/time bits'
        debug['weather_state'] = format_weather_state(weather_state)
        return {'kind': 'part1', 'debug': debug}

    stats['part2'] += 1
    stats['triplets_completed'] += 1

    for i in range(14):
        weather_state[28 + i] = a[i + 1]

    debug['action'] = 'stored third 14 weather bits; decrypting'
    debug['weather_state'] = format_weather_state(weather_state)

    uiBitCnt = 0
    ucTemp = 0
    uiCnt = 1
    cipher = [0] * 5
    key = [0] * 5

    while uiCnt < 42:
        if uiCnt != 7:
            ucTemp >>= 1
            if weather_state[uiCnt] == 1:
                ucTemp |= 0x80
            uiBitCnt += 1
            if (uiBitCnt & 7) == 0:
                cipher[(uiBitCnt >> 3) - 1] = ucTemp
        uiCnt += 1

    uiBitCnt = 0
    ucTemp = 0
    while uiCnt < 82:
        ucTemp >>= 1
        if weather_state[uiCnt] == 1:
            ucTemp |= 0x80
        uiBitCnt += 1
        if (uiBitCnt & 7) == 0:
            key[(uiBitCnt >> 3) - 1] = ucTemp
        uiCnt += 1

    plain = decrypt(cipher, key)
    check = ((((plain[2] & 0x0F) << 8) | plain[1]) << 4) | (plain[0] >> 4)

    stats['meteotime_attempts'] += 1

    if check != 0x2501:
        stats['meteotime_fail'] += 1
        return {
            'kind': 'decrypt_fail',
            'debug': debug,
            'cipher': cipher,
            'key': key,
            'plain': plain,
            'check': check,
        }

    w0 = flip_byte(((plain[3] & 0x0F) << 4) | ((plain[2] & 0xF0) >> 4))
    w1 = flip_byte(((plain[4] & 0x0F) << 4) | ((plain[3] & 0xF0) >> 4))
    w2 = flip_byte(((plain[0] & 0x0F) << 4) | ((plain[4] & 0xF0) >> 4))
    w2 = (w2 & 0xFC) | 0x02
    payload = (w0 << 16) | (w1 << 8) | w2

    mapped = decode_weather_info(payload)
    mapped = add_region_section(mapped, r)

    stats['meteotime_ok'] += 1

    return {
        'kind': 'decrypt_ok',
        'debug': debug,
        'payload': payload,
        'mapped': mapped,
        'cipher': cipher,
        'key': key,
        'plain': plain,
        'check': check,
    }


# ============================================================
# Output helpers
# ============================================================

def print_stats(stats: dict):
    print(
        f"STATS    : valid_minutes={stats['valid_minutes']}  "
        f"invalid_minutes={stats['invalid_minutes']}  "
        f"skipped_for_meteotime={stats['skipped_for_meteotime']}  "
        f"part0={stats['part0']}  part1={stats['part1']}  part2={stats['part2']}  "
        f"triplets_started={stats['triplets_started']}  "
        f"triplets_completed={stats['triplets_completed']}  "
        f"attempts={stats['meteotime_attempts']}  "
        f"ok={stats['meteotime_ok']}  fail={stats['meteotime_fail']}"
    )


def print_time_decode(decoded: DecodedMinute):
    print(
        f"DCF OK   : {decoded.dd:02d}.{decoded.mo:02d}.{decoded.yy:04d} "
        f"{decoded.hh:02d}:{decoded.mm:02d} "
        f"(weekday={decoded.weekday}, tz={decoded.timezone})"
    )


def print_meteotime_result(result):
    kind = result['kind']

    if kind == 'skip_invalid':
        print("TRIPLET  : skipped before triplet assembly")
        print("METEO    : skipped")
        print(f"REASON   : {result['reason']}")
        return

    d = result['debug']
    print(
        f"TRIPLET  : minute={d['minute_raw']:02d}  "
        f"minute-1={d['minute_minus_1']:02d}  "
        f"triplet={d['triplet']:02d}  part={d['part']}"
    )
    print(f"ACTION   : {d['action']}")
    print(f"STATE    : {d['weather_state']}")

    if kind == 'part0':
        print("METEO    : waiting for more triplet parts")
        return

    if kind == 'part1':
        print("METEO    : waiting for final triplet part")
        return

    if kind == 'decrypt_fail':
        print("METEO FAIL:")
        print(f"  check   : 0x{result['check']:04X} (expected 0x2501)")
        print('  cipher  : ' + ' '.join(f'{x:02X}' for x in result['cipher']))
        print('  key     : ' + ' '.join(f'{x:02X}' for x in result['key']))
        print('  plain   : ' + ' '.join(f'{x:02X}' for x in result['plain']))
        return

    if kind == 'decrypt_ok':
        r = result['mapped']
        print("METEO OK  :")
        print(f"  payload : {r['payload_hex']}")
        print(f"  region  : {r['region_id']} - {r['region_name']} ({r['forecast_days']}-Tagesprognose)")
        print(f"  section : {r['section_id']} - {r['day_label']} / {r['section_kind']}")
        if r['display_day_code'] is None:
            print(f"  day     : {r['display_day_weather']}")
        else:
            print(f"  day     : {r['display_day_weather']} (Code {r['display_day_code']})")
        if r['display_night_code'] is None:
            print(f"  night   : {r['display_night_weather']}")
        else:
            print(f"  night   : {r['display_night_weather']} (Code {r['display_night_code']})")
        print(f"  temp    : {r['temp_text']} (Code {r['temp_code']})")
        print(f"  anomaly : {r['anomaly_bit']}")
        if r.get('is_low_wind_section') and not r.get('section7_high_override'):
            if r['anomaly_bit'] == 0:
                print(
                    f"  wind    : {r['wind_direction']}, Stärke {r['wind_force']} "
                    f"(Code {r['wind_full_code']}, dir={r['wind_dir_code']}, force={r['wind_force_code']})"
                )
            else:
                print("  wind    : -")
        print(f"  detail  : {r['section_value_text']}")
        print(f"  bits8..11: {r['bits8_11_mode']} -> {r['bits8_11_text']}")
        print('  cipher  : ' + ' '.join(f'{x:02X}' for x in result['cipher']))
        print('  key     : ' + ' '.join(f'{x:02X}' for x in result['key']))
        print('  plain   : ' + ' '.join(f'{x:02X}' for x in result['plain']))
        print(f"  info    : {r['info0_hex']} {r['info1_hex']} {r['info2_hex']}")


# ============================================================
# Main loop
# ============================================================

def main():
    print("DCF77 live Meteotime decoder on GPIO17")
    print("Live decoder aligned to the current reference offline decoder")
    print("LOW phase is treated as pulse, HIGH phase as pause/minute marker")
    print(f"Web status JSON: {STATUS_JSON_PATH}")
    print("Waiting for minute frames...\n")

    bits59: List[int] = []
    weather_state = [0] * 82

    stats = {
        'valid_minutes': 0,
        'invalid_minutes': 0,
        'skipped_for_meteotime': 0,
        'part0': 0,
        'part1': 0,
        'part2': 0,
        'triplets_started': 0,
        'triplets_completed': 0,
        'meteotime_attempts': 0,
        'meteotime_ok': 0,
        'meteotime_fail': 0,
    }

    shared_state = create_initial_web_state(STATUS_JSON_PATH)
    shared_lock = threading.Lock()
    stop_event = threading.Event()

    update_shared_state(shared_state, shared_lock, lambda s: s['stats'].update(stats.copy()))
    atomic_write_json(STATUS_JSON_PATH, shared_state)

    writer_thread = threading.Thread(
        target=status_writer_loop,
        args=(shared_state, shared_lock, STATUS_JSON_PATH, stop_event),
        daemon=True,
    )
    writer_thread.start()

    last_edge = time.time()
    last_state = dcf.value

    try:
        while True:
            state = dcf.value

            if state != last_state:
                now = time.time()
                ms = (now - last_edge) * 1000.0
                current_second = len(bits59) + 1 if len(bits59) < 59 else 59
                period_ms = ms

                if last_state == 0:
                    bit = classify_pulse(ms)
                    ts = time.strftime("%H:%M:%S")

                    if bit is not None:
                        bits59.append(bit)
                        print(f"{ts}  pulse={ms:7.2f} ms  bit={bit}  count={len(bits59):02d}")
                        update_shared_state(shared_state, shared_lock, lambda s: s['rf'].update({
                            'signal_state': int(state),
                            'pulse_ms': round(ms, 2),
                            'period_ms': round(period_ms, 2),
                            'bit': bit,
                            'bit_count': len(bits59),
                            'second': len(bits59),
                            'current_bits': ''.join(str(x) for x in bits59),
                            'current_triplet_state': format_weather_state(weather_state),
                            'last_event': 'pulse',
                            'last_event_at': iso_now_local(),
                            'minute_marker_detected': False,
                        }))
                    else:
                        if ms >= 25:
                            print(f"{ts}  unusual pulse={ms:7.2f} ms  count={len(bits59):02d}")
                            update_shared_state(shared_state, shared_lock, lambda s: s['rf'].update({
                                'signal_state': int(state),
                                'pulse_ms': round(ms, 2),
                                'period_ms': round(period_ms, 2),
                                'bit': None,
                                'bit_count': len(bits59),
                                'second': current_second,
                                'current_bits': ''.join(str(x) for x in bits59),
                                'current_triplet_state': format_weather_state(weather_state),
                                'last_event': 'unusual_pulse',
                                'last_event_at': iso_now_local(),
                                'minute_marker_detected': False,
                                'last_unusual_pulse_ms': round(ms, 2),
                            }))

                else:
                    ts = time.strftime("%H:%M:%S")
                    update_shared_state(shared_state, shared_lock, lambda s: s['rf'].update({
                        'signal_state': int(state),
                        'pause_ms': round(ms, 2),
                        'period_ms': round(period_ms, 2),
                        'bit_count': len(bits59),
                        'second': current_second,
                        'current_bits': ''.join(str(x) for x in bits59),
                        'current_triplet_state': format_weather_state(weather_state),
                        'last_event': 'pause',
                        'last_event_at': iso_now_local(),
                    }))

                    if ms > 1500:
                        print(f"\n{ts}  === minute marker detected ===")
                        print(f"BITS     : collected={len(bits59)}")
                        update_shared_state(shared_state, shared_lock, lambda s: s['rf'].update({
                            'minute_marker_detected': True,
                            'last_minute_marker_at': iso_now_local(),
                            'pause_ms': round(ms, 2),
                            'period_ms': round(period_ms, 2),
                            'second': 0,
                        }))

                        if len(bits59) == 59:
                            decoded, err = decode_time_minute(bits59)

                            if decoded is not None:
                                stats['valid_minutes'] += 1
                                print_time_decode(decoded)
                                update_shared_state(shared_state, shared_lock, lambda s: s['dcf'].update(decoded_minute_to_status(decoded)))

                                result = process_meteotime_minute(decoded, weather_state, stats)
                                print_meteotime_result(result)
                                update_shared_state(
                                    shared_state,
                                    shared_lock,
                                    lambda s: s['meteotime'].update(
                                        meteotime_result_to_status(
                                            result,
                                            previous_last_good_mapped=s['meteotime'].get('last_good_mapped'),
                                            previous_last_good_updated_at=s['meteotime'].get('last_good_updated_at'),
                                        )
                                    )
                                )
                                print_stats(stats)
                                print()

                            else:
                                stats['invalid_minutes'] += 1
                                print(f"DCF FAIL : {err}")
                                update_shared_state(shared_state, shared_lock, lambda s: s['dcf'].update({
                                    'valid': False,
                                    'decoded_at': iso_now_local(),
                                    'datetime_text': None,
                                    'date_text': None,
                                    'time_text': None,
                                    'weekday_index': None,
                                    'weekday_name': None,
                                    'timezone': None,
                                    'timezone_name_de': None,
                                    'sync_ok': False,
                                    'reason': err,
                                    'raw_bits': ''.join(str(x) for x in bits59),
                                }))
                                print_stats(stats)
                                print()
                        else:
                            stats['invalid_minutes'] += 1
                            print("DCF FAIL : expected 59 bits")
                            update_shared_state(shared_state, shared_lock, lambda s: s['dcf'].update({
                                'valid': False,
                                'decoded_at': iso_now_local(),
                                'datetime_text': None,
                                'date_text': None,
                                'time_text': None,
                                'weekday_index': None,
                                'weekday_name': None,
                                'timezone': None,
                                'timezone_name_de': None,
                                'sync_ok': False,
                                'reason': 'expected 59 bits',
                                'raw_bits': ''.join(str(x) for x in bits59),
                            }))
                            print_stats(stats)
                            print()

                        update_shared_state(shared_state, shared_lock, lambda s: s['stats'].update(stats.copy()))
                        bits59 = []
                        update_shared_state(shared_state, shared_lock, lambda s: s['rf'].update({
                            'bit': None,
                            'bit_count': 0,
                            'second': 0,
                            'current_bits': '',
                            'current_triplet_state': format_weather_state(weather_state),
                            'last_event': 'minute_reset',
                            'last_event_at': iso_now_local(),
                        }))

                last_edge = now
                last_state = state

            time.sleep(0.001)

    except KeyboardInterrupt:
        print("\nStopped.")
        print_stats(stats)
    finally:
        stop_event.set()
        writer_thread.join(timeout=2.0)
        update_shared_state(shared_state, shared_lock, lambda s: s['stats'].update(stats.copy()))
        atomic_write_json(STATUS_JSON_PATH, shared_state)


if __name__ == "__main__":
    main()
