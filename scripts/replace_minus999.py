#!/usr/bin/env python3
"""Replace any value equal to -999 (numeric) in a CSV with an empty string.
Creates a backup of the original file with extension .bak before overwriting.
Usage:
    python replace_minus999.py -i "path/to/file.csv"

This script uses the stdlib csv module and streams rows so it works for large files.
"""

import csv
import argparse
import shutil
import tempfile
import os


def process(input_path: str, inplace: bool = True, encoding: str = "utf-8") -> int:
    input_path = os.path.abspath(input_path)
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Prepare temp output
    fd, temp_path = tempfile.mkstemp(prefix="replace_minus999_", suffix=".csv", dir=os.path.dirname(input_path))
    os.close(fd)

    replacements = 0

    # Detect dialect from a sample
    with open(input_path, "r", newline="", encoding=encoding, errors="replace") as fh:
        sample = fh.read(8192)
        fh.seek(0)
        try:
            dialect = csv.Sniffer().sniff(sample)
        except Exception:
            dialect = csv.excel

        reader = csv.reader(fh, dialect)
        with open(temp_path, "w", newline="", encoding=encoding) as out_fh:
            writer = csv.writer(out_fh, dialect)
            for row in reader:
                new_row = []
                for cell in row:
                    replaced = cell
                    # try numeric comparison; if cell is parseable as float and equals -999, replace
                    stripped = cell.strip()
                    if stripped == "":
                        new_row.append(cell)
                        continue
                    try:
                        num = float(stripped)
                        if num == -999.0:
                            replaced = ""
                            replacements += 1
                    except Exception:
                        # fallback exact string match (handles cases like '-999')
                        if stripped == "-999":
                            replaced = ""
                            replacements += 1
                    new_row.append(replaced)
                writer.writerow(new_row)

    # Backup original and move temp into place if inplace
    if inplace:
        backup_path = input_path + ".bak"
        shutil.copy2(input_path, backup_path)
        shutil.move(temp_path, input_path)
        print(f"Replaced {replacements} cells. Original backed up to: {backup_path}")
        return replacements
    else:
        out_name = input_path.replace('.csv', '.replaced.csv')
        shutil.move(temp_path, out_name)
        print(f"Replaced {replacements} cells. Output written to: {out_name}")
        return replacements


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Replace -999 values in a CSV with empty strings")
    parser.add_argument("-i", "--input", required=True, help="Path to input CSV file")
    parser.add_argument("--no-inplace", dest="inplace", action="store_false", help="Do not overwrite original; write to a new file")
    parser.add_argument("--encoding", default="utf-8", help="File encoding (default utf-8). Try utf-8-sig if you have BOM)")
    args = parser.parse_args()

    try:
        n = process(args.input, inplace=args.inplace, encoding=args.encoding)
    except Exception as e:
        print("Error:", e)
        raise
