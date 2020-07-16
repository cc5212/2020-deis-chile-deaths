#!/bin/bash

cd src
for file in *.pig; do
    echo "Exec file" $file
    pig $file
done
