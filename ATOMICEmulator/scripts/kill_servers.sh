kill -9 `ps ax | grep "serve" | grep -v grep | awk '{print $1}'`
