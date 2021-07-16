STATUS="$(systemctl is-active nifi.service)"
if [ "${STATUS}" = "active" ]; then
	echo "NIFI is Working"
else
	echo "NIFI service is not running in shdldiffenvm2...." | mail -s "NIFI stopped working" vamshi.krishna@thedatateam.in
	exit 1
fi	
