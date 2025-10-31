#!/usr/bin/env python3
"""
merge_inmet_focos.py
Une o arquivo INMET (Belo Horizonte Pampulha) com o arquivo de focos (gerado por remove_columns.py)
por data e hora UTC — apenas linhas de focos cujo município corresponda a Belo Horizonte serão consideradas.

Uso:
  python merge_inmet_focos.py <inmet_csv> <focos_no_cols_csv> [output_csv]

O script detecta o cabeçalho do INMET pulando linhas de metadados, suporta o formato de data/hora do INMET
(Ex.: Data: 2024/01/02, Hora UTC: 0600 UTC) e o formato do arquivo de focos (data DD/MM/YYYY, hora 'HHMM UTC').
Gera um CSV com colunas do INMET seguidas das colunas de focos prefixadas com 'f_'.
"""
from pathlib import Path
import csv
import sys
from datetime import datetime
import io
import unicodedata


def normalize_text(s: str) -> str:
    if s is None:
        return ""
    s = s.strip()
    s = s.casefold()
    # remove accents
    s = ''.join(c for c in unicodedata.normalize('NFD', s) if unicodedata.category(c) != 'Mn')
    return s


def find_inmet_header_and_rows(path: Path):
    # try common encodings (utf-8, latin1, cp1252)
    text = None
    for enc in ('utf-8', 'latin-1', 'cp1252'):
        try:
            text = path.read_text(encoding=enc)
            break
        except Exception:
            continue
    if text is None:
        raise ValueError(f'Não foi possível ler o arquivo {path} com as codificações testadas')
    lines = text.splitlines()
    header_idx = None
    for i, line in enumerate(lines):
        # header line contains Data and Hora (Hora UTC)
        if 'Data' in line and 'Hora' in line:
            header_idx = i
            break
    if header_idx is None:
        raise ValueError('Cabeçalho do INMET não encontrado (linha com "Data" e "Hora").')
    data_lines = lines[header_idx:]
    # use csv reader with semicolon delimiter
    buf = io.StringIO('\n'.join(data_lines))
    reader = csv.DictReader(buf, delimiter=';')
    rows = list(reader)
    return reader.fieldnames, rows


def read_focos(path: Path):
    # focos file expected to be comma-separated
    with path.open('r', encoding='utf-8', newline='') as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        return reader.fieldnames, rows


def parse_inmet_datetime(date_str: str, hour_str: str):
    # date_str example: '2024/01/02'
    # hour_str example: '0600 UTC' or '0000 UTC'
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str.strip(), '%Y/%m/%d')
    except Exception:
        return None
    if not hour_str:
        return None
    # remove ' UTC' suffix if present
    h = hour_str.replace('UTC', '').strip()
    # h like '0600' or '06:00'
    h = h.replace(':', '')
    try:
        hour = int(h)
    except Exception:
        return None
    # hour is HHMM -> convert to hour integer
    hour_int = hour // 100
    return datetime(d.year, d.month, d.day, hour_int)


def parse_focos_datetime(date_str: str, hora_utc_str: str):
    # date_str example: '02/01/2024'
    # hora_utc_str example: '0600 UTC' or '0600'
    if not date_str:
        return None
    try:
        d = datetime.strptime(date_str.strip(), '%d/%m/%Y')
    except Exception:
        return None
    if not hora_utc_str:
        return None
    h = hora_utc_str.replace('UTC', '').strip()
    h = h.replace(':', '')
    try:
        hour = int(h)
    except Exception:
        return None
    hour_int = hour // 100
    return datetime(d.year, d.month, d.day, hour_int)


def merge(inmet_path: Path, focos_path: Path, out_path: Path):
    print('Lendo INMET:', inmet_path)
    inmet_fields, inmet_rows = find_inmet_header_and_rows(inmet_path)
    print(f'Linhas INMET lidas: {len(inmet_rows)}')

    print('Lendo focos:', focos_path)
    focos_fields, focos_rows = read_focos(focos_path)
    print(f'Linhas focos lidas: {len(focos_rows)}')

    # Filter out unwanted focos fields (remove risco de fogo) and normalize focos by datetime and municipio == Belo Horizonte
    # define nomes de risco a serem removidos (normalizados)
    risco_names = {normalize_text(x) for x in ('risco_fogo', 'risco de fogo', 'riscofogo', 'risco')}

    # build filtered focos_fields (preserve order)
    focos_fields_filtered = []
    for f in focos_fields:
        if normalize_text(f).replace(' ', '') in risco_names:
            continue
        if normalize_text(f) in risco_names:
            continue
        focos_fields_filtered.append(f)

    focos_index = {}
    for r in focos_rows:
        municipio = r.get('municipio') or r.get('Municipio') or r.get('MUNICIPIO')
        if municipio is None:
            municipio = ''
        if normalize_text(municipio) != normalize_text('Belo Horizonte'):
            continue
        dt = parse_focos_datetime(r.get('data', ''), r.get('hora_utc', ''))
        if dt is None:
            continue
        # create a reduced record containing only filtered fields
        reduced = {f: r.get(f, '') for f in focos_fields_filtered}
        focos_index.setdefault(dt, []).append(reduced)

    print(f'Focos em Belo Horizonte (horas únicas): {len(focos_index)}')

    # build output header: INMET fields + prefixed focos fields
    pref = 'f_'
    focos_prefixed = [pref + f for f in focos_fields_filtered]
    out_fields = list(inmet_fields) + focos_prefixed

    written = 0
    with out_path.open('w', encoding='utf-8-sig', newline='') as wf:
        writer = csv.writer(wf)
        writer.writerow(out_fields)
        for ir in inmet_rows:
            inmet_date = ir.get(inmet_fields[0])  # 'Data'
            inmet_hour = ir.get(inmet_fields[1])  # 'Hora UTC'
            dt = parse_inmet_datetime(inmet_date, inmet_hour)
            if dt is None:
                continue
            matches = focos_index.get(dt)
            if not matches:
                continue
            # for each matching foco, write a merged row
            for fr in matches:
                out_row = [ir.get(f, '') for f in inmet_fields]
                out_row += [fr.get(f, '') for f in focos_fields_filtered]
                writer.writerow(out_row)
                written += 1

    print(f'Linhas escritas no arquivo de saída: {written}')


if __name__ == '__main__':
    if len(sys.argv) < 3:
        print('Uso: python merge_inmet_focos.py <inmet_csv> <focos_no_cols_csv> [output_csv]')
        sys.exit(1)
    inmet = Path(sys.argv[1])
    focos = Path(sys.argv[2])
    if len(sys.argv) >= 4:
        out = Path(sys.argv[3])
    else:
        out = focos.with_name(focos.stem + '_merged_with_inmet.csv')

    merge(inmet, focos, out)
