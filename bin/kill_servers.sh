#!/bin/bash
#kill -9 `ps ax | grep "serve" | grep -v grep | awk '{print $1}'`
#echo "Servers stopped..."
sudo pkill -f "python"
echo "Watch stopped..."
