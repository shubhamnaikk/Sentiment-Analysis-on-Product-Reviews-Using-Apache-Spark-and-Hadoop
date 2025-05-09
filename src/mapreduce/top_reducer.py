#!/usr/bin/env python3
import sys
import heapq

def main():
    # Dictionary to store top items by type
    top_items = {
        "PRODUCT": [],
        "CATEGORY": []
    }
    
    # Read sorted data from mapper
    for line in sys.stdin:
        try:
            item_type, key, avg_rating, count = line.strip().split('\t')
            avg_rating = float(avg_rating)
            count = int(count)
            
            # Store in appropriate list
            if item_type in top_items:
                # Use a tuple with avg_rating as the first element for sorting
                top_items[item_type].append((avg_rating, key, count))
        except ValueError:
            continue
    
    # Sort and output top products (by rating)
    print("TOP_PRODUCTS_BY_RATING")
    sorted_products = sorted(top_items["PRODUCT"], reverse=True)
    for i, (avg_rating, product_id, count) in enumerate(sorted_products[:10], 1):
        print(f"{i}\t{product_id}\t{avg_rating:.2f}\t{count}")
    
    # Sort and output top products (by popularity/count)
    print("\nTOP_PRODUCTS_BY_POPULARITY")
    pop_sorted_products = sorted(top_items["PRODUCT"], key=lambda x: x[2], reverse=True)
    for i, (avg_rating, product_id, count) in enumerate(pop_sorted_products[:10], 1):
        print(f"{i}\t{product_id}\t{avg_rating:.2f}\t{count}")
    
    # Sort and output top categories (by rating)
    print("\nTOP_CATEGORIES_BY_RATING")
    sorted_categories = sorted(top_items["CATEGORY"], reverse=True)
    for i, (avg_rating, category, count) in enumerate(sorted_categories, 1):
        print(f"{i}\t{category}\t{avg_rating:.2f}\t{count}")
    
    # Sort and output top categories (by popularity/count)
    print("\nTOP_CATEGORIES_BY_POPULARITY")
    pop_sorted_categories = sorted(top_items["CATEGORY"], key=lambda x: x[2], reverse=True)
    for i, (avg_rating, category, count) in enumerate(pop_sorted_categories, 1):
        print(f"{i}\t{category}\t{avg_rating:.2f}\t{count}")

if __name__ == "__main__":
    main()