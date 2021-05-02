#!/usr/bin/env bash

set -e

root="/home/datadev/${USER}"

repoName="structured-streaming-learning"
moduleName=$(basename $(dirname $(dirname $(realpath ${BASH_SOURCE}))))
modelName=$(basename $(dirname $(realpath ${BASH_SOURCE})))

param=${@}

sparkSubmit="/usr/local/spark3.0.1/bin/spark-submit"
deployMode="client"
sparkConfig=$(cat <<-END
  --executor-memory 20G \
  --executor-cores 2 \
  --driver-memory 20G \
  --total-executor-cores 40 \
  --conf spark.network.timeout=12000s \
  --conf spark.executor.heartbeatInterval=100s \
  --conf spark.yarn.maxAppAttempts=1
END
)
target="target/scala-2.12/${modelName}.jar"
sparkAppName="${moduleName}_${modelName} ${param}"

logDir="${root}/logs/${repoName}/${moduleName}_${modelName}"
mkdir -p ${logDir}
logPath="${logDir}/${param}.log"
logPath=${logPath// /.}

cd `dirname $0` # move to directory where this shell script is in
${sparkSubmit} \
    --name "${sparkAppName}" \
    --deploy-mode ${deployMode} \
    ${sparkConfig} \
    ${target} ${param} \
    |& tee ${logPath}

sparkSubmitExit=${PIPESTATUS[0]}
exit ${sparkSubmitExit}