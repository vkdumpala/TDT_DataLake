date=$1
baseApiUrl="https://southindia.azuredatabricks.net/api/2.0"
netRCFile=/home/diffen/.netrc
openssl rsautl -inkey /opt/diffen/databricks/key.txt -decrypt</mnt/diffen-share/.databricks.bin >${netRCFile}
postBody='{"job_id": 157,"notebook_params":{"eod_time":"'$date'"}}'
curl --netrc-file $netRCFile -s -X POST -d "${postBody}"  ${baseApiUrl}/jobs/run-now

