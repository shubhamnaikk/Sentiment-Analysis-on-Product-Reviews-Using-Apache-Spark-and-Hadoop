#!/usr/bin/env python3
import sys

def main():
    current_key = None
    total = 0
    count = 0

    for line in sys.stdin:
        try:
            key, rating = line.strip().split('\t')
            rating = float(rating)

            if key == current_key:
                total += rating
                count += 1
            else:
                if current_key is not None:
                    print(f"{current_key}\t{total / count}")
                current_key = key
                total = rating
                count = 1
        except ValueError:
            continue

    if current_key is not None:
        print(f"{current_key}\t{total / count}")

if __name__ == "__main__":
    main()