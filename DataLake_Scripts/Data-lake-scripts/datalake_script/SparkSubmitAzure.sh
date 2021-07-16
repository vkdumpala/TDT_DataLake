#!/usr/bin/env bash
BASEDIR=$(dirname "$0")
source ${BASEDIR}/env.profile

#<hadoop username> <hive resources> <eod Marker> <business Day> <parameter xml hdfs path> <fulldump|incremental> <rerun Tables List> <databricks region>
export HADOOP_USER_NAME=$1
export HIVE_CONFIG_RESOURCES=$2

eodMarker=$3
businessDay=$4
paramXMLPath=$5
loadType=$6
rerunTablesList=$7

netRCFile=/home/diffen/.netrc
openssl rsautl -inkey /opt/diffen/databricks/key.txt -decrypt</mnt/diffen-share/.databricks.bin >${netRCFile}

baseApiUrl="https://southindia.azuredatabricks.net/api/2.0"

getSRIPostBody(){
    
    eodMarker=$1
    businessDay=$2
    paramXMLPath=$3
    loadType=$4
    rerunTablesList=$5
    
    jq -n --arg runName 'SRI' \
    --arg sparkVersion '4.3.x-scala2.11' \
    --arg nodeTypeId 'Standard_E16_v3' \
    --arg minWorkers '3' \
    --arg maxWorkers '6' \
    --arg eodMarker "$eodMarker" \
    --arg businessDay "${businessDay}" \
    --arg paramXMLPath $paramXMLPath \
    --arg loadType $loadType \
    --arg rerunTablesList "$rerunTablesList" \
    '{
run_name: $runName,
    new_cluster: {
            spark_version: $sparkVersion,
            node_type_id: $nodeTypeId,
            num_workers: null,
            autoscale: {
                    min_workers: $minWorkers,
                    max_workers: $maxWorkers
            },
"init_scripts": { "dbfs": { "destination": "dbfs:/databricks/temp/set_spark_params.sh" }}
},
    spark_submit_task: {
            parameters: [
                    "--conf",
                    "spark.sql.shuffle.partitions=20",
                    "--conf",
                    "spark.default.parallelism=20",
                    "--class",
                    "com.tdt.diffen.processor.Diffen",
                    "--driver-memory",
                    "8g",
                    "--executor-memory",
                    "16g",
                    "dbfs:/diffen/IngestionProcessingV2-assembly-1.0.jar",
                    $eodMarker,
                    $businessDay,
                    $paramXMLPath,
                    $loadType,
                    $rerunTablesList
            ]
    }
    }'
}

getJobState() {
    jobId=$1
    curl --netrc-file $netRCFile -s ${baseApiUrl}/jobs/runs/get-output?run_id=$jobId | jq .metadata.state.life_cycle_state
}

getJobResultState() {
    jobId=$1
    curl --netrc-file $netRCFile -s ${baseApiUrl}/jobs/runs/get-output?run_id=$jobId | jq .metadata.state.result_state
}

#Submit spark job to databricks
postBody=`getSRIPostBody "$eodMarker" "$businessDay" $paramXMLPath $loadType $rerunTablesList`
jobId=`curl --netrc-file $netRCFile -s -X POST -d "${postBody}" ${baseApiUrl}/jobs/runs/submit | jq .run_id`
[ $? -ne 0 ] && {
	echo "Unable to retrieve JobID from databricks. Exiting.."
	exit 1
}

echo "SPARK SUBMIT job id : [$jobId]"

lifeCycleState=`getJobState $jobId`

if [[ $lifeCycleState =~ "null" ]]
then
        echo "Unable to retrieve cluster state. Exitting.."
        exit 1
fi

while [[ $lifeCycleState =~ "PENDING" ]] || [[ $lifeCycleState =~ "RUNNING" ]] || [[ $lifeCycleState =~ "TERMINATING" ]]
do
    lifeCycleState=`getJobState $jobId`
    sleep 20;
done

resultState=`getJobResultState $jobId`
if [[ $resultState =~ "SUCCESS" ]]
then
    exit 0
else
    curl --netrc-file $netRCFile -s  ${baseApiUrl}/jobs/runs/get-output?run_id=$jobId | jq .metadata.state.state_message
    exit 1
fi
