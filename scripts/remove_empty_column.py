#!/usr/bin/env python3
"""Remove columns whose header name is empty or unnamed from a CSV.

Usage:
    python remove_empty_column.py -i path/to/file.csv

This script:
 - Detects CSV dialect with csv.Sniffer
 - Finds header columns whose name is empty/whitespace or starts with 'Unnamed'
 - Rewrites the CSV without those columns, creating a backup with suffix .bak
 - Streams rows so it works for large files
"""

import argparse
import csv
import os
import shutil
import tempfile


def remove_empty_header_columns(input_path: str, inplace: bool = True, encoding: str = "utf-8") -> int:
    input_path = os.path.abspath(input_path)
    if not os.path.exists(input_path):
        raise FileNotFoundError(f"Input file not found: {input_path}")

    # Prepare temporary output
    fd, temp_path = tempfile.mkstemp(prefix="remove_empty_col_", suffix=".csv", dir=os.path.dirname(input_path))
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
            raise ValueError("Input CSV is empty")

        # Determine indices to remove: header that is empty/whitespace or 'Unnamed...'
        remove_idxs = [i for i, col in enumerate(header) if (col is None) or (str(col).strip() == "") or str(col).strip().lower().startswith("unnamed")]

        if not remove_idxs:
            # nothing to remove; clean up temp and return
            os.remove(temp_path)
            print("No unnamed/empty header columns found.")
            return 0

        # Build output header
        new_header = [col for i, col in enumerate(header) if i not in remove_idxs]

        with open(temp_path, "w", newline="", encoding=encoding) as outf:
            writer = csv.writer(outf, dialect)
            writer.writerow(new_header)
            removed_count_rows = 0
            for row in reader:
                # Pad row if shorter than header
                if len(row) < len(header):
                    row = row + [""] * (len(header) - len(row))
                new_row = [cell for i, cell in enumerate(row) if i not in remove_idxs]
                writer.writerow(new_row)
                removed_count_rows += 1

    # Backup and move into place
    if inplace:
        backup_path = input_path + ".bak"
        shutil.copy2(input_path, backup_path)
        shutil.move(temp_path, input_path)
        print(f"Removed {len(remove_idxs)} empty/unnamed column(s). Original backed up to: {backup_path}")
        return len(remove_idxs)
    else:
        out_name = input_path.replace('.csv', '.cleaned.csv')
        shutil.move(temp_path, out_name)
        print(f"Removed {len(remove_idxs)} empty/unnamed column(s). Output written to: {out_name}")
        return len(remove_idxs)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Remove unnamed/empty header columns from a CSV")
    parser.add_argument("-i", "--input", required=True, help="Path to input CSV file")
    parser.add_argument("--no-inplace", dest="inplace", action="store_false", help="Do not overwrite original; write to a new file")
    parser.add_argument("--encoding", default="utf-8", help="File encoding (default utf-8). Try utf-8-sig if needed)")
    args = parser.parse_args()

    try:
        remove_empty_header_columns(args.input, inplace=args.inplace, encoding=args.encoding)
    except Exception as e:
        print("Error:", e)
        raise
