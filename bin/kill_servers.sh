kill -9 `ps ax | grep "serve" | grep -v grep | awk '{print $1}'`
echo "Servers stopped..."
kill -9 `ps -u seecs | grep "python" | grep -v grep | awk '{print $1}'`
echo "Watches stopped..."
