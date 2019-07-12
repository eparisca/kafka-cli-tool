#!/bin/bash

# strict
set -o errexit -o pipefail -o noclobber -o nounset
set -x

########################################
# Configuration
########################################
source kafka_tool.cfg

readonly progversion="v1.3"
readonly progname="Kafka Tool"

########################################
# Error definitions
########################################
readonly INVALID_ARGUMENT_ERROR=1

########################################
# Print usage instructions
########################################
function usage() {  
    echo "
NAME
    ${progname} ${progversion} -- Query Kafka/Zookeeper data and metadata

SYNOPSIS 
    ${0} -l
    ${0} -g CONSUMER_GROUP_NAME
    ${0} -t TOPIC_NAME
    ${0} -a TOPIC_NAME
    ${0} -m TOPIC_NAME PARTITION OFFSET
    ${0} -n TOPIC_NAME NUMBER_OF_REPLICAS NUMBER_OF_PARTITIONS
    ${0} -p TOPIC_NAME
    ${0} -r TOPIC_NAME CONSUMER_GROUP_NAME DELTA_OFFSET
    ${0} -d TOPIC_NAME

DESCRIPTION
    This script runs queries to retrieve data and metadata from Kafka & Zookeeper. 

OPTIONS
    The following options are available:
    -l      List consumer groups
    -g      Describe a specific consumer group, including current lag
    -t      Describe a specific topic
    -a      Consume all messages from a specific topic
    -m      Consume the message from the specified topic, at the specified partition and offset
    -n      Create a new topic
    -p      Purge/empty a topic (without deleting it)
    -r      Rewind the topic offsets of a consumer group by the specified number
    -d      Delete a specific topic
    -h      Print this help message and exit.

USAGE
    ${0} -l
    ${0} -d my_group_name

CONFIGURATION
    This script relies on kafka_tool.cfg for environment configuration.
"
}

function logger() {
    local log_level="${1}"
    local message="${2}"
    echo "${log_level}" "${message}" >>/dev/stderr
}
########################################
# List consumer groups
########################################
function list_consumer_groups() {
    logger "INFO" "Querying old consumers: "
    ${kafka_dir}/kafka-consumer-groups.sh --zookeeper ${zookeeper} --list
    logger "INFO" "Querying new consumers: "
    ${kafka_dir}/kafka-consumer-groups.sh --bootstrap-server ${broker} --list
}

########################################
# Describe a particular consumer group
# @param string ${group_name}
########################################
function describe_consumer_group() {
    local group_name=$1
    logger "INFO" "Querying old consumers: "
    ${kafka_dir}/kafka-consumer-groups.sh --zookeeper ${zookeeper} --describe --group ${group_name}
    logger "INFO" "Querying new consumers: "
    ${kafka_dir}/kafka-consumer-groups.sh --bootstrap-server ${broker} --describe --group ${group_name}
}

########################################
# Describe a particular topic
# @param string ${topic_name}
########################################
function describe_topic() {
    local topic_name=$1
    ${kafka_dir}/kafka-topics.sh --zookeeper ${zookeeper} --describe --topic ${topic_name}
    # ${kafka_dir}/kafka-topics.sh --bootstrap-server ${broker} --describe --topic ${topic_name}
}

########################################
# Create a kafka topic from zookeeper
# @param string ${topic_name}
# @param int ${replicas}
# @param int ${partitions}
########################################
function create_topic {
    local topic_name=$1
    local replicas=$2
    local partitions=$3
    ${kafka_dir}/kafka-topics.sh --zookeeper ${zookeeper} --create --replication-factor ${replicas} --partitions ${partitions} --topic ${topic_name}
}

########################################
# Purge a kafka topic
# @param string ${topic_name}
########################################
function purge_topic {
    local topic_name=$1
    ./empty_topic.sh -t ${topic_name}
}

########################################
# Fully delete a kafka topic from the broker and zookeeper
# @param string ${topic_name}
########################################
function delete_topic() {
    local topic_name=$1
    ${kafka_dir}/kafka-topics.sh --zookeeper ${zookeeper} --delete --topic ${topic_name}
    # ${zookeeper_dir}/zkCli.sh ${zookeeper} <<< "rmr /brokers/topics/${topic_name}"
}

########################################
# Consume from particular topic
# @param string ${topic_name}
########################################
function consume_topic() {
    local topic_name=$1
    #${kafka_dir}/kafka-console-consumer.sh --zookeeper ${zookeeper} --topic ${topic_name} --timeout-ms 2000
    ${kafka_dir}/kafka-console-consumer.sh --bootstrap-server ${broker} --topic ${topic_name} --offset ${offset} --timeout-ms 2000
}

########################################
# Consume the message from the specified topic, at the specified partition and offset
# @param string ${topic_name}
# @param int ${partition}
# @param int ${offset}
########################################
function consume_topic_at() {
    local topic_name=$1
    local partition=$2
    local offset=$3
    #${kafka_dir}/kafka-console-consumer.sh --zookeeper ${zookeeper} --topic ${topic_name}
    ${kafka_dir}/kafka-console-consumer.sh --bootstrap-server ${broker} --topic ${topic_name} --partition ${partition} --offset ${offset} --max-messages 1
}

########################################
# Rewind offsets for a consumer group on specific topic
# @param string ${topic_name}
# @param string ${group_name}
# @param int ${delta_offset}
# @example rewind_offsets "engagement" "Comtrak" 100
########################################
function rewind_offsets() {
    local topic_name=$1
    local group_name=$2
    local delta_offset=$3
    local temp_filename=".reset_plan.csv"
    # write CSV
    ${kafka_dir}/kafka-consumer-groups.sh --bootstrap-server ${broker} --describe --group ${group_name} | grep ${topic_name} | \
        awk -v delta_offset=${delta_offset} '{ print $1, $2, $3 - delta_offset }' OFS="," > ${temp_filename}
    # reset offsets
    ${kafka_dir}/kafka-consumer-groups.sh --bootstrap-server ${broker} --reset-offsets --group ${group_name} --topic ${topic_name} --from-file ${temp_filename} --execute
    # remove CSV file
    rm ${temp_filename}
}

while getopts "hlg:t:c:m:n:r:d:" opt; do
    case ${opt} in
        l)  list_consumer_groups
            ;;
        g)  describe_consumer_group "${OPTARG}"
            ;;
        t)  describe_topic "${OPTARG}"
            ;;
        c)  consume_topic "${OPTARG}"
            ;;
        m)  consume_topic_at "${OPTARG}" $3 $4
            ;;
        n)  create_topic "${OPTARG}" $3 $4
            ;;
        p)  purge_topic "${OPTARG}" $3
            ;;
        d)  delete_topic "${OPTARG}"
            ;;
        r)  rewind_offsets "${OPTARG}" $3 $4
            ;;
        h)  usage
            ;;
        *)  usage
            exit ${INVALID_ARGUMENT_ERROR}
            ;;
    esac
done

exit 0
