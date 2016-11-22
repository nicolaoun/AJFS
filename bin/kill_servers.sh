kill -9 `ps ax | grep "serve" | grep -v grep | awk '{print $1}'`
echo "Servers stopped..."
pkill -f watch_cjfs.py
echo "Watch stopped..."
