#!/bin/bash
# inspect_file.sh

if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <filename>"
    exit 1
fi

FILE="$1"
echo "Inspecting file: $FILE"
echo "---------------------------------"
echo "File type (using 'file' command):"
file "$FILE"
echo

echo "File size:"
du -h "$FILE"
echo

echo "First 10 bytes (hex dump):"
head -c 10 "$FILE" | hexdump -C
echo

echo "Trying to determine file format and content:"
if file "$FILE" | grep -q "gzip"; then
    echo "File appears to be gzip compressed."
    echo "Decompressed first 100 bytes:"
    gunzip -c "$FILE" 2>/dev/null | head -c 100 | od -c
    echo
    echo "First few lines after decompression:"
    gunzip -c "$FILE" 2>/dev/null | head -n 2
else
    echo "File does not appear to be gzip compressed."
    echo "First 100 bytes:"
    head -c 100 "$FILE" | od -c
    echo
    echo "First few lines:"
    head -n 2 "$FILE"
fi