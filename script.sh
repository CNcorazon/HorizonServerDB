#!/bin/bash
echo "server starts"
echo "start 60 clients"

#/home/admin1/HorizonServer/./main fork &
for ((i=0;i<8;i++))
do
{
    /home/admin1/HorizonChain/./main
}&
done