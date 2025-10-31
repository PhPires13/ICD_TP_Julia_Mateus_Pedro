#!/usr/bin/env python3
"""One-hot encode the `f_bioma` column in a CSV.

Creates a backup of the original file as .bak and writes the transformed CSV in-place by default.

Usage:
    python one_hot_encode_bioma.py -i path/to/file.csv

Options:
    --column    column name to encode (default: f_bioma)
    --no-inplace write output to path.cleaned.csv instead of overwriting
    --encoding  file encoding (default utf-8)

This script is streaming-friendly and uses two passes: first to collect categories, second to write output.
"""

import argparse
import csv
import os
import shutil
import tempfile
import unicodedata
import re


def sanitize_colname(s: str) -> str:
    # normalize accents, remove non-alnum, replace spaces with underscore
    if s is None:
        s = ""
    s = str(s).strip()
    s = unicodedata.normalize("NFKD", s)
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^0-9A-Za-z]+", "_", s)
    s = s.strip("_")
    if s == "":
        s = "bioma"
    return s


def collect_categories(input_path: str, column_name: str, encoding: str = "utf-8"):
    categories = []
    seen = set()
    with open(input_path, "r", newline="", encoding=encoding, errors="replace") as fh:
        sample = fh.read(8192)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.excel
        reader = csv.reader(fh, dialect)
        try:
            header = next(reader)
        except StopIteration:
            raise ValueError("Input CSV is empty")

        # find index (case-sensitive exact match first, then case-insensitive)
        idx = None
        if column_name in header:
            idx = header.index(column_name)
        else:
            lowered = [h.lower() for h in header]
            if column_name.lower() in lowered:
                idx = lowered.index(column_name.lower())

        if idx is None:
            raise ValueError(f"Column '{column_name}' not found in header")

        for row in reader:
            if len(row) <= idx:
                val = ""
            else:
                val = row[idx].strip()
            if val != "" and val not in seen:
                seen.add(val)
                categories.append(val)

    return header, idx, categories


def one_hot_encode(input_path: str, column_name: str = "f_bioma", inplace: bool = True, encoding: str = "utf-8"):
    input_path = os.path.abspath(input_path)
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    header, col_idx, categories = collect_categories(input_path, column_name, encoding=encoding)
    if not categories:
        print("No non-empty categories found in column; nothing to encode.")
        return 0

    # sanitize category names to make safe column names and ensure uniqueness
    sanitized = []
    used = set()
    for cat in categories:
        name = sanitize_colname(cat)
        base = name
        i = 1
        while name in used:
            name = f"{base}_{i}"
            i += 1
        used.add(name)
        sanitized.append((cat, name))

    # prepare temp output
    fd, temp_path = tempfile.mkstemp(prefix="onehot_bioma_", suffix=".csv", dir=os.path.dirname(input_path))
    os.close(fd)

    with open(input_path, "r", newline="", encoding=encoding, errors="replace") as inf:
        sample = inf.read(8192)
        inf.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.excel

        reader = csv.reader(inf, dialect)
        orig_header = next(reader)

        # Build new header: keep original columns except the f_bioma column, then append one-hot columns
        new_header = [c for i, c in enumerate(orig_header) if i != col_idx]
        new_header += [f"bioma_{name}" for (_, name) in sanitized]

        with open(temp_path, "w", newline="", encoding=encoding) as outf:
            writer = csv.writer(outf, dialect)
            writer.writerow(new_header)
            for row in reader:
                # pad
                if len(row) < len(orig_header):
                    row = row + [""] * (len(orig_header) - len(row))
                base_row = [cell for i, cell in enumerate(row) if i != col_idx]
                val = row[col_idx].strip() if len(row) > col_idx else ""
                onehots = []
                for (cat, name) in sanitized:
                    onehots.append("1" if val != "" and val == cat else "0")
                writer.writerow(base_row + onehots)

    # backup and move
    if inplace:
        backup_path = input_path + ".bak"
        shutil.copy2(input_path, backup_path)
        shutil.move(temp_path, input_path)
        print(f"One-hot encoded {len(sanitized)} categories for column '{column_name}'. Backup: {backup_path}")
        return len(sanitized)
    else:
        out_name = input_path.replace('.csv', '.onehot.csv')
        shutil.move(temp_path, out_name)
        print(f"One-hot encoded {len(sanitized)} categories for column '{column_name}'. Output: {out_name}")
        return len(sanitized)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="One-hot encode a categorical column (default f_bioma) in a CSV")
    parser.add_argument("-i", "--input", required=True, help="Path to input CSV file")
    parser.add_argument("--column", default="f_bioma", help="Column name to one-hot encode (default f_bioma)")
    parser.add_argument("--no-inplace", dest="inplace", action="store_false", help="Do not overwrite original; write to a new file")
    parser.add_argument("--encoding", default="utf-8", help="File encoding (default utf-8)")
    args = parser.parse_args()

    try:
        one_hot_encode(args.input, column_name=args.column, inplace=args.inplace, encoding=args.encoding)
    except Exception as e:
        print("Error:", e)
        raise
