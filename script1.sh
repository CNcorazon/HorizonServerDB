#!/bin/bash
echo "server starts"
echo "start 60 clients"

#/home/admin1/HorizonServer/./main fork &
for ((i=0;i<455;i++))
do
{
    /home/dell/Desktop/HorizonChain/./main
}&
done