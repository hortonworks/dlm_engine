#!/bin/sh
# HORTONWORKS DATAPLANE SERVICE AND ITS CONSTITUENT SERVICES
#
# (c) 2016-2018 Hortonworks, Inc. All rights reserved.
#
# This code is provided to you pursuant to your written agreement with Hortonworks, which may be the terms of the
# Affero General Public License version 3 (AGPLv3), or pursuant to a written agreement with a third party authorized
# to distribute this code.  If you do not have a written agreement with Hortonworks or with an authorized and
# properly licensed third party, you do not have any rights to this code.
#
# If this code is provided to you under the terms of the AGPLv3:
# (A) HORTONWORKS PROVIDES THIS CODE TO YOU WITHOUT WARRANTIES OF ANY KIND;
# (B) HORTONWORKS DISCLAIMS ANY AND ALL EXPRESS AND IMPLIED WARRANTIES WITH RESPECT TO THIS CODE, INCLUDING BUT NOT
#    LIMITED TO IMPLIED WARRANTIES OF TITLE, NON-INFRINGEMENT, MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE;
# (C) HORTONWORKS IS NOT LIABLE TO YOU, AND WILL NOT DEFEND, INDEMNIFY, OR HOLD YOU HARMLESS FOR ANY CLAIMS ARISING
#    FROM OR RELATED TO THE CODE; AND
# (D) WITH RESPECT TO YOUR EXERCISE OF ANY RIGHTS GRANTED TO YOU FOR THE CODE, HORTONWORKS IS NOT LIABLE FOR ANY
#    DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, PUNITIVE OR CONSEQUENTIAL DAMAGES INCLUDING, BUT NOT LIMITED TO,
#    DAMAGES RELATED TO LOST REVENUE, LOST PROFITS, LOSS OF INCOME, LOSS OF BUSINESS ADVANTAGE OR UNAVAILABILITY,
#    OR LOSS OR CORRUPTION OF DATA.

if [ $# -ne 4 ]; then
        printf "Usage: $0 <KNOX_PROVIDERURL> <BEACON_URL> <KNOX_USERNAME> <KNOX_PASSWORD> 
	                 KNOX_PROVIDERURL : Specify the Beacon Knox SSO Provider URL \\n \
			 BEACON_URL       : Specify complete Beacon URL [http/https]://<beacon-server>:<beacon-port> \\n \
                        KNOX_USERNAME    : Specify the Knox Username \\n \
                        KNOX_PASSWORD    : Specify the Knox Password \n"
        exit 1
fi

KNOX_PROVIDERURL=$1
BEACON_URL=$2
KNOX_USERNAME=$3
KNOX_PASSWORD=$4
BEACON_API="api/beacon/cluster/list"

CURL='/usr/bin/curl'
CURLARGS=" --silent -iku"
DATE_WITH_TIME=`date +%Y%m%d-%H%M%S`
KNOX_OUT="/tmp/knox_${DATE_WITH_TIME}.out"
BEACON_OUT="/tmp/beacon_${DATE_WITH_TIME}.out"

echo "******* Fetching the HADOOP JWT Token from the cluster *******"
rm -rf ${KNOX_OUT}
echo $CURL $CURLARGS ${KNOX_USERNAME}:${KNOX_PASSWORD} "${KNOX_PROVIDERURL}?originalUrl=${BEACON_URL}/${BEACON_API} -o ${KNOX_OUT}\n"
eval $CURL $CURLARGS ${KNOX_USERNAME}:${KNOX_PASSWORD} "${KNOX_PROVIDERURL}?originalUrl=${BEACON_URL}/${BEACON_API} -o ${KNOX_OUT}"
HADOOP_JWT=`cat ${KNOX_OUT} | grep hadoop-jwt | cut -d'=' -f2| cut -d';' -f1`
if [ -z "${HADOOP_JWT}" ]; then
    echo "HADOOP_JWT from Knox SSO is empty. Please check the ${KNOX_OUT} for results"
else
    echo "HADOOP_JWT from Knox SSO : ${HADOOP_JWT}\n"
    echo "******* Verifying Beacon Knox SSO Setup *******"

    KNOX_CURL_ARGS=" -iL -u : --cookie"
    rm -rf ${BEACON_OUT}
    echo "$CURL $KNOX_CURL_ARGS "hadoop-jwt=${HADOOP_JWT}" ${BEACON_URL}/${BEACON_API} -o ${BEACON_OUT}\n"
    eval $CURL $KNOX_CURL_ARGS "hadoop-jwt=${HADOOP_JWT}" ${BEACON_URL}/${BEACON_API} -o ${BEACON_OUT}
    if [ -f ${BEACON_OUT} ]; then
        if cat ${BEACON_OUT} | grep -q 'totalResults'; then
           echo "Beacon Rest API results stored in ${BEACON_OUT}\n"
           echo "******* Knox SSO successfully configured with DLM Engine *******\n"
        else
           echo "******* Knox SSO not successfully configured with DLM Engine *******\n"
           echo "Please verify ${BEACON_OUT} for details\n"
        fi
    fi
fi