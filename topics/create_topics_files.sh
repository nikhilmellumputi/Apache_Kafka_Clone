#!/bin/sh

file=$(cat topics/topics_names.txt)

for line in $file
do 
    touch "topics/$line.txt"
done
