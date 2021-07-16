#!/usr/bin/env bash
BASEDIR=$(dirname "$0")
source $BASEDIR/env.profile

#<hadoop username> <hive resources> <source> <country> <nas config dir> <nas incoming dir> <nas appl dir> <hdfs config dir>

export HADOOP_USER_NAME=$1
export HIVE_CONFIG_RESOURCES=$2
source=$3
country=$4
nas_incoming_dir=$5
nas_config_dir=$6
nas_appl_dir=$7
hdfs_config_dir=$8
config_filename=$9
dt=`date +%Y-%m-%d`

script_name=`basename $0 | cut -d "." -f1`
log_file=${nas_appl_dir}/${script_name}_${source}_${country}_$dt.log

netRCFile=/home/diffen/.netrc
openssl rsautl -inkey /opt/diffen/databricks/key.txt -decrypt</mnt/diffen-share/.databricks.bin >${netRCFile}

baseApiUrl="https://${DATABRICKS_REGION}.azuredatabricks.net/api/2.0"

getPostBody(){
    
    HIVE_CONFIG_RESOURCES=$1
    paramXMLPath=$2
    updatedTablesXmlPath=$3

    jq -n --arg runName 'SCHEMA_EVOLUTION' \
    --arg sparkVersion '4.3.x-scala2.11' \
    --arg nodeTypeId 'Standard_E16_v3' \
    --arg minWorkers '2' \
    --arg maxWorkers '6' \
    --arg paramXMLPath $paramXMLPath \
    --arg HIVE_CONFIG_RESOURCES $HIVE_CONFIG_RESOURCES \
    --arg updatedTablesXmlPath ${updatedTablesXmlPath} \
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
    timeout_seconds: "3600",
    spark_submit_task: {
            parameters: [
                    "--class",
                    "com.tdt.diffen.processor.SchemaEvolutionV2",
                    "--conf",
                    "spark.sql.catalogImplementation=hive",
                    "--driver-memory",
                    "8g",
                    "--executor-memory",
                    "16g",
                    "dbfs:/diffen/IngestionProcessingV2-assembly-1.0.jar",
                    "--generate-backward-compatible-table-config",
                    "--new-table-config-path",
                    $updatedTablesXmlPath,
                    "--param-config-path",
                    $paramXMLPath
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
    curl --netrc-file $netRCFile -s ${baseApiUrl}/2.0/jobs/runs/get-output?run_id=$jobId | jq .metadata.state.result_state
}

#Submit spark job to databricks
#postBody=`getPostBody "$HIVE_CONFIG_RESOURCES" ${hdfs_config_dir}/${source}_${country}_param.xml ${hdfs_config_dir}/${source}_${country}_tables_config_updated.xml`
postBody=`getPostBody "$HIVE_CONFIG_RESOURCES" ${hdfs_config_dir}/${source}_${country}_param.xml ${hdfs_config_dir}/${config_filename}`
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

#Backup the existing tableConfig file & re-name the new tableConfig file in NAS layer
mv ${nas_config_dir}/${source}_${country}_tables_config.xml ${nas_config_dir}/${source}_${country}_tables_config_$dt.xml
if [ $? -ne 0 ]; then
    echo "ERROR `date +'%Y-%m-%d %T'` : FAILED to run rename NAS" > $log_file
    exit 1;
    else
    echo "INFO `date +'%Y-%m-%d %T'` : SUCCESSFULLY ran rename NAS" >> $log_file
fi

$HADOOP_HOME/bin/hadoop fs -get ${hdfs_config_dir}/${source}_${country}_tables_config.xml ${nas_config_dir}/
if [ $? -ne 0 ]; then
    echo "ERROR `date +'%Y-%m-%d %T'` : FAILED to run backup NAS" >> $log_file
    exit 1;
    else
    echo "INFO `date +'%Y-%m-%d %T'` : SUCCESSFULLY ran backup NAS" >> $log_file
fi

#Move the '.xml' file for Incoming Directory to Config Directory in NAS layer
for xml in `ls ${nas_incoming_dir}/*.xml`
do
file=`basename $xml`
mv $xml ${nas_appl_dir}/${file}_$dt
if [ $? -ne 0 ]; then
    echo "ERROR `date +'%Y-%m-%d %T'` : FAILED to run move NAS" >> $log_file
    exit 1;
    else
    echo "INFO `date +'%Y-%m-%d %T'` : SUCCESSFULLY ran move NAS" >> $log_file
fi
done

rm -f $log_file

exit 0

################################################################################################################################################
# END
################################################################################################################################################
