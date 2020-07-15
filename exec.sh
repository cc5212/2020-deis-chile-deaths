#!/bin/bash

count=0
for file in *.pig; do
    count=$((count+1))
    echo "Exec file" $file
    pig $file
done
echo $count
