#!/usr/bin/env python3
import sys

def main():
    # This mapper will read output from the first reducer
    # and emit different keys for products vs categories
    for line in sys.stdin:
        try:
            key, avg_rating, count = line.strip().split('\t')
            
            # Determine if this is a product or category
            if key.startswith("CATEGORY_"):
                category_name = key[9:]  # Remove the 'CATEGORY_' prefix
                # Emit with a prefix for the reducer to group by type
                print(f"CATEGORY\t{category_name}\t{avg_rating}\t{count}")
            else:
                # This is a product
                # Only include products with sufficient reviews (e.g., at least 5)
                if int(count) >= 5:  
                    print(f"PRODUCT\t{key}\t{avg_rating}\t{count}")
        except ValueError:
            continue

if __name__ == "__main__":
    main()