#!/usr/bin/env python3
"""Transform f_data into f_day,f_month,f_year and f_hora_utc into hour_sin,hour_cos (cyclical).

Usage:
    python transform_date_time.py -i path/to/file.csv

The script:
 - Detects CSV dialect
 - Locates columns `f_data` and `f_hora_utc` (case-insensitive)
 - Replaces those columns in-place with new numeric columns
 - Creates a backup with .bak suffix
"""

import argparse
import csv
import math
import os
import re
import shutil
import tempfile


def parse_date_to_dmy(s: str):
    if not s:
        return ("", "", "")
    parts = re.findall(r"\d+", s)
    if not parts:
        return ("", "", "")
    # Expect day, month, year in that order
    day = parts[0] if len(parts) >= 1 else ""
    month = parts[1] if len(parts) >= 2 else ""
    year = parts[2] if len(parts) >= 3 else ""
    if year and len(year) == 2:
        # assume 2000s
        year = str(2000 + int(year))
    return (day, month, year)


def parse_time_to_fraction(s: str):
    """Return fraction of day (0..1) for time string like '1700 UTC' or '04:00' or '4'"""
    if not s:
        return None
    digits = re.findall(r"\d+", s)
    if not digits:
        return None
    token = digits[0]
    # token may be '1700' or '4' or '04'
    if len(token) >= 3:
        # treat last two as minutes
        hh = int(token[:-2])
        mm = int(token[-2:])
    else:
        hh = int(token)
        mm = 0
    if not (0 <= hh <= 23 and 0 <= mm <= 59):
        # try to clamp or return None
        hh = hh % 24
        mm = mm % 60
    fraction = (hh + mm / 60.0) / 24.0
    return fraction


def time_fraction_to_sin_cos(frac: float):
    # frac in [0,1)
    angle = 2 * math.pi * frac
    return math.sin(angle), math.cos(angle)


def transform_file(input_path: str, inplace: bool = True, encoding: str = "utf-8"):
    input_path = os.path.abspath(input_path)
    if not os.path.exists(input_path):
        raise FileNotFoundError(input_path)

    fd, temp_path = tempfile.mkstemp(prefix="transform_dt_", suffix=".csv", dir=os.path.dirname(input_path))
    os.close(fd)

    with open(input_path, "r", newline="", encoding=encoding, errors="replace") as inf:
        sample = inf.read(8192)
        inf.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.excel
        reader = csv.reader(inf, dialect)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("Empty CSV")

        # find indices for f_data and f_hora_utc (case-insensitive)
        lowered = [h.lower() for h in header]
        data_idx = None
        hora_idx = None
        if 'f_data' in lowered:
            data_idx = lowered.index('f_data')
        if 'f_hora_utc' in lowered:
            hora_idx = lowered.index('f_hora_utc')

        if data_idx is None and hora_idx is None:
            os.remove(temp_path)
            print("No f_data or f_hora_utc columns found; nothing to do.")
            return 0

        # Build new header: replace data_idx with f_day,f_month,f_year and hora_idx with hour_sin,hour_cos
        new_header = []
        for i, col in enumerate(header):
            if i == data_idx:
                new_header.extend(["f_day", "f_month", "f_year"])
            elif i == hora_idx:
                new_header.extend(["hour_sin", "hour_cos"])
            else:
                new_header.append(col)

        # if both indices exist and hora_idx > data_idx, note that indices shift when writing but we handle per row

        with open(temp_path, "w", newline="", encoding=encoding) as outf:
            writer = csv.writer(outf, dialect)
            writer.writerow(new_header)
            for row in reader:
                # pad
                if len(row) < len(header):
                    row = row + [""] * (len(header) - len(row))
                out_row = []
                for i, cell in enumerate(row):
                    if i == data_idx:
                        day, month, year = parse_date_to_dmy(cell.strip())
                        out_row.extend([day, month, year])
                    elif i == hora_idx:
                        frac = parse_time_to_fraction(cell.strip())
                        if frac is None:
                            out_row.extend(["", ""])
                        else:
                            s, c = time_fraction_to_sin_cos(frac)
                            out_row.extend([f"{s:.6f}", f"{c:.6f}"])
                    else:
                        out_row.append(cell)
                writer.writerow(out_row)

    if inplace:
        backup_path = input_path + ".bak"
        shutil.copy2(input_path, backup_path)
        shutil.move(temp_path, input_path)
        print(f"Transformed date/time columns. Backup at {backup_path}")
        return 1
    else:
        out_name = input_path.replace('.csv', '.dt.csv')
        shutil.move(temp_path, out_name)
        print(f"Transformed date/time columns. Output at {out_name}")
        return 1


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Transform f_data and f_hora_utc into numeric components')
    parser.add_argument('-i', '--input', required=True, help='Path to CSV')
    parser.add_argument('--no-inplace', dest='inplace', action='store_false', help='Do not overwrite original')
    parser.add_argument('--encoding', default='utf-8', help='File encoding')
    args = parser.parse_args()
    try:
        transform_file(args.input, inplace=args.inplace, encoding=args.encoding)
    except Exception as e:
        print('Error:', e)
        raise
