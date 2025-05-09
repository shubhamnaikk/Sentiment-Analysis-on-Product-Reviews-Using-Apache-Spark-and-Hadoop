import sys

def emit(key, total, count):
    if count > 0:
        avg = total / count
        print(f"{key}\t{avg:.2f}\t{count}")

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
                    emit(current_key, total, count)
                current_key = key
                total = rating
                count = 1
        except ValueError:
            continue  # skip bad lines

    if current_key is not None:
        emit(current_key, total, count)

if __name__ == "__main__":
    main()