STATUS="$(systemctl is-active hive.service)"
if [ "${STATUS}" = "active" ]; then
	echo "HIVI is Working"
else
	echo "Hive service is not running in shdldiffenvm2...." | mail -s "HIVE stopped working" vamshi.krishna@thedatateam.in -- -r az.streamsets-alerts@starhealth.biz
	exit 1
fi	
