#!/bin/bash

function missing_var() {
	var_name=${1}
	echo "Please set the ${var_name} environment variable to point to the appropriate installation directory" >&2
	exit 1
}

if [ -z "${SEQPIG_HOME}" ]; then
	SEQPIG_HOME=`dirname $0`
	SEQPIG_HOME="${SEQPIG_HOME}/../"
fi

if [ -z "${PIG_HOME}" ]; then
	missing_var PIG_HOME
fi

if [ -z "${JAVA_HOME}" ]; then
	missing_var JAVA_HOME
fi

HADOOP="${HADOOP:-hadoop}"

type -P "${HADOOP}" &>/dev/null || {
	echo  "warning! hadoop not found in PATH";
	if [ -z "${HADOOP_HOME}" ]; then
		echo "HADOOP_HOME not set.  Pig may not work in distributed mode";
	else
		HADOOP="${HADOOP_HOME}/bin/hadoop";
	fi
}

SEQPIG_VERSION=0.6-SNAPSHOT

SEQPIG_LIBJARS=$(find ${SEQPIG_HOME}/target -name '*.jar' -print | tr '\n' ,)

echo "SEQPIG_LIBJARS: $SEQPIG_LIBJARS"


