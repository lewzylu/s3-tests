kill -9 $(ps -ef|grep start.py|gawk '$0 !~/grep/ {print $2}' |tr -s '\n' ' ')
