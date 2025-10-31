#!/usr/bin/env python3
"""
remove_columns.py
Removes specified columns from a CSV and writes a new CSV with suffix _no_cols.csv.
Usage:
  python remove_columns.py <input_csv> [output_csv]
If output_csv is omitted, the script writes to <input_basename>_no_cols.csv next to the input file.
"""
import csv
import sys
from pathlib import Path
from datetime import datetime, timedelta

COLUMNS_TO_REMOVE = {"pais", "satelite", "id_area_industrial"}


def remove_columns(input_path: Path, output_path: Path):
    removed = []
    with input_path.open("r", encoding="utf-8", newline="") as rf:
        reader = csv.reader(rf)
        try:
            header = next(reader)
        except StopIteration:
            print(f"Arquivo vazio: {input_path}")
            return

        # Map header names to their indices
        lower_map = {h.strip(): i for i, h in enumerate(header)}
        # Find index of data_pas column (case-insensitive)
        data_pas_idx = None
        for i, h in enumerate(header):
            if h.strip().lower() == "data_pas":
                data_pas_idx = i
                break

        # Find indices to drop by case-insensitive matching
        to_drop_idx = set()
        for i, h in enumerate(header):
            if h.strip().lower() in COLUMNS_TO_REMOVE:
                to_drop_idx.add(i)
                removed.append(h.strip())

        if not to_drop_idx:
            print(f"Nenhuma das colunas {sorted(COLUMNS_TO_REMOVE)} foi encontrada no arquivo. Saindo sem alterar.")
            return

        # Build new header: if data_pas is present, replace it with two columns
        new_header = []
        for i, h in enumerate(header):
            if i in to_drop_idx:
                continue
            if i == data_pas_idx:
                # replace data_pas with date and hora_utc
                new_header.append("data")
                new_header.append("hora_utc")
            else:
                new_header.append(h)

        with output_path.open("w", encoding="utf-8", newline="") as wf:
            writer = csv.writer(wf)
            writer.writerow(new_header)
            # Copy remaining rows
            for row in reader:
                # If row shorter than header, pad
                if len(row) < len(header):
                    row = row + [""] * (len(header) - len(row))

                # Try to parse and round data_pas if present
                parsed_dt = None
                original = ""
                if data_pas_idx is not None and data_pas_idx < len(row):
                    original = row[data_pas_idx].strip()
                    if original:
                        parsed_dt = _round_datetime_string_to_hour(original)

                # Build output row, replacing data_pas by two columns when applicable
                out_row = []
                for i in range(len(header)):
                    if i in to_drop_idx:
                        continue
                    if i == data_pas_idx:
                        if parsed_dt is not None:
                            date_str = parsed_dt.strftime("%d/%m/%Y")
                            time_str = parsed_dt.strftime("%H%M") + " UTC"
                            out_row.append(date_str)
                            out_row.append(time_str)
                        else:
                            # If not parseable, keep original in first column and leave time empty
                            out_row.append(original)
                            out_row.append("")
                    else:
                        out_row.append(row[i])

                writer.writerow(out_row)

    print(f"Removidas colunas: {removed}")
    print(f"Arquivo de saída escrito em: {output_path}")


def _round_datetime_string_to_hour(s: str) -> datetime | None:
    """Tenta parsear uma string de data/hora e arredondar para a hora mais próxima.

    Retorna um objeto datetime (com minutos/segundos zerados, possivelmente incrementado em 1 hora)
    ou None se não foi possível parsear.
    """
    # formatos comuns esperados
    formats = ["%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%dT%H:%M"]
    dt = None
    for fmt in formats:
        try:
            dt = datetime.strptime(s, fmt)
            break
        except Exception:
            continue
    if dt is None:
        return None

    # compute rounding: if minutes >= 30 -> round up, else down
    if dt.minute >= 30 or (dt.minute == 29 and dt.second >= 30):
        dt = (dt.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
    else:
        dt = dt.replace(minute=0, second=0, microsecond=0)

    return dt


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Uso: python remove_columns.py <input_csv> [output_csv]")
        sys.exit(2)
    input_csv = Path(sys.argv[1])
    if not input_csv.exists():
        print(f"Arquivo não encontrado: {input_csv}")
        sys.exit(1)
    if len(sys.argv) >= 3:
        output_csv = Path(sys.argv[2])
    else:
        output_csv = input_csv.with_name(input_csv.stem + "_no_cols" + input_csv.suffix)

    remove_columns(input_csv, output_csv)
