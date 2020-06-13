#!/bin/bash
set -e
OUTPUT=bitcoin_blocks.tsv
for days in $(bash -c "echo {$((($(date +%Y) - 2008)*366))..1}"); do
    date="$(date -d "$days days ago" +%Y%m%d)"
    if [ $date -lt 20090109 ] && [ $date -ne 20090103 ]; then
        continue
    fi
    file="blockchair_bitcoin_blocks_${date}.tsv.gz"
    if [ ! -f "$file" ]; then
        echo -e "\nOK: Downloading $file"
        torsocks wget --quiet --show-progress "https://gz.blockchair.com/bitcoin/blocks/$file"
    else
        echo -ne "OK: already have $file\x0d"
    fi
done
echo
last="$(zcat -1 "$file" | tail -1 | cut -f 1 -d '	')"
if [ "$(( $(cat $OUTPUT 2>/dev/null|wc -l) -2))" -eq "$last" ]; then
    echo "OK: already have the expected number of lines in $OUTPUT; not re-merging"
else
    echo "Merging data for $last blocks into $OUTPUT..."
    zcat blockchair_bitcoin_blocks_20090103.tsv.gz|head -1 > $OUTPUT
    for file in blockchair_bitcoin_blocks_20??????.tsv.gz; do
        zcat "$file"|grep -v ^id
    done | pv -l -s "$last" | sort -n >> $OUTPUT
fi
lines="$(cat $OUTPUT|wc -l)"
if [ "$(($lines - 2))" -eq "$last" ]; then
    echo "OK: synced up to block $last"
else
    echo "error: we have $lines lines in $OUTPUT, but the last block is $last"
    exit 1
fi
