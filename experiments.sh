#!/bin/sh
counter=0
while [ $counter -le 10 ]
do
echo $counter
((counter++))
done