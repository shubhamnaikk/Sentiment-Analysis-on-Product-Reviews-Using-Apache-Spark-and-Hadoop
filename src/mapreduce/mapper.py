import sys
import csv

def main():
    reader = csv.reader(sys.stdin)
    header = next(reader, None)  # skip header

    for row in reader:
        try:
            product_id = row[1].strip()
            category = row[4].strip()  # assuming category is column 4 (adjust if needed)
            rating = float(row[3].strip())

            if product_id:
                print(f"{product_id}\t{rating}")
            if category:
                print(f"CATEGORY_{category}\t{rating}")
        except (IndexError, ValueError):
            continue  # skip rows with missing or bad data

if __name__ == "__main__":
    main()