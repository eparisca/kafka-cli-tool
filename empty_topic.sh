#!/bin/bash

# strict
set -o errexit -o pipefail -o noclobber -o nounset
set -x

readonly progversion="v2.3"
readonly progname="Kafka Topic Cleaner"

# Error definitions
readonly INVALID_ARGUMENT_ERROR=1
readonly MISSING_ARGUMENT_ERROR=2

########################################
# Configuration
########################################
source kafka_tool.cfg

# Timeout to allow configuration changes 
readonly timeout=300

# Usage instructions
function usage() {  
    echo "
NAME
    ${progname} ${progversion} -- Empty the contents of one or more Kafka topics

SYNOPSIS 
    ${0} [-f] -t TOPIC_NAME_1 [-t TOPIC_NAME_N]

OPTIONS
    The following options are available:
    -t      Topic name
    -f      Force run without prompting for confirmation
    -h      Print this help message and exit.

EXAMPLES
    ${0} -t \"test-topic\"
    ${0} -t \"test-topic-1\" -t \"test-topic-2\" -t \"test-topic-3\"

CONFIGURATION
    This script relies on kafka_tool.cfg for environment configuration.
"
}

declare -d topic_names=()

# Parse arguments
force=false
while getopts "t:fh" opt; do
    case ${opt} in
        t)  topic_names[${#topic_names[@]}]="${OPTARG}"
            ;;
        f)  force=true
            ;;
        h)  usage && exit 0
            ;;
    esac
done

########################################
# @param string topic_name
########################################
function empty_topic() {
    local topic_name=${1}
    ${kafka_dir}/kafka-configs.sh --zookeeper ${zookeeper} --alter --entity-type topics --entity-name ${topic_name} --add-config retention.ms=1000
}

########################################
# Wait
########################################
function wait() {
    echo "Waiting ${timeout} seconds to allow configuration change to be applied."
    sleep ${timeout}
}

########################################
# @param string topic_name
########################################
function restore_configuration() {
    local topic_name=${1}
    ${kafka_dir}/kafka-configs.sh --zookeeper ${zookeeper} --alter --entity-type topics --entity-name ${topic_name} --delete-config retention.ms
}

########################################
# @param string topic_name
########################################
function get_offsets() {
    local topic_name=${1}
    ${kafka_dir}/kafka-run-class.sh kafka.tools.GetOffsetShell --broker-list ${broker} --time -1 --topic ${topic_name}
}

########################################
# @param array topic_names
########################################
function main() {
    local topic_names=("$@")

    if [[ "${#topic_names[@]}" == "0" ]]; then
        echo "Error: A minimum of one TOPIC_NAME is required"
        usage 
        exit ${MISSING_ARGUMENT_ERROR}
    fi

    echo "${progname}"
    echo ""
    echo "Zookeeper: ${zookeeper}"
    echo "Kafka Broker: ${broker}"
    echo "Topic(s): ${topic_names[@]}"

    if [[ ! ${force} ]];
        # Prompt user before proceeding
        echo "WARNING: Emptying a topic is irreversible. "
        read -p "Are you sure that you want to proceed? [y/n]: " answer
        if [[ "${answer}" != "y" ]]; then
            echo "Cancelled."
            exit 0
        fi
    fi

    echo "Starting cleaning."
    for topic_name in "${topic_names[@]}"; do
        empty_topic ${topic_name}
    done
    wait
    echo "Cleaning completed."

    echo "Restoring configuration."
    for topic_name in "${topic_names[@]}"; do
        restore_configuration ${topic_name}
    done
    wait

    echo "Topic count and offset: "
    for topic_name in "${topic_names[@]}"; do
        get_offsets ${topic_name}
    done
    echo "Done."
}

main "${topic_names[@]}"

# TODO: 
# ${kafka_dir}/kafka-delete-records.sh --bootstrap-server ${broker} --offset-json-file offsets.json
# offsets.json : 
# echo <<HEREDOC
# {"partitions": [{"topic": “${topic_name}", "partition": 0, "offset": -1}], "version":1 }
# HEREDOC > offsets.json
# rm offsets.json
