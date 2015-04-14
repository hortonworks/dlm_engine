/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
(function () {
  'use strict';
  var module = angular.module('app.services.entity.model', []);

  module.factory('EntityModel', ["X2jsService", "$cookieStore", function(X2jsService, $cookieStore) {

    var EntityModel = {};

    EntityModel.json = null;
    EntityModel.detailsPageModel = null;

    EntityModel.identifyType = function(json) {
      if(json.feed) { EntityModel.type = "feed"; }
      else if(json.cluster) { EntityModel.type = "cluster"; }
      else if(json.process) { EntityModel.type = "process"; }
      else { EntityModel.type = 'Type not recognized'; }
    };

    EntityModel.getJson = function(xmlString) {
      EntityModel.json = X2jsService.xml_str2json( xmlString );
      return EntityModel.identifyType(EntityModel.json);
    };

    var userName;
    if($cookieStore.get('userToken') !== null &&$cookieStore.get('userToken') !== undefined ){
      userName = $cookieStore.get('userToken').user;
    }else{
      userName = "";
    }

    EntityModel.clusterModel = {
      cluster:{
        tags: "",
        interfaces:{
          interface:[
            {
              _type:"readonly",
              _endpoint:"hftp://sandbox.hortonworks.com:50070",
              _version:"2.2.0"
            },
            {
              _type:"write",
              _endpoint:"hdfs://sandbox.hortonworks.com:8020",
              _version:"2.2.0"

            },
            {
              _type:"execute",
              _endpoint:"sandbox.hortonworks.com:8050",
              _version:"2.2.0"

            },
            {
              _type:"workflow",
              _endpoint:"http://sandbox.hortonworks.com:11000/oozie/",
              _version:"4.0.0"

            },
            {
              _type:"messaging",
              _endpoint:"tcp://sandbox.hortonworks.com:61616?daemon=true",
              _version:"5.1.6"

            }
          ]
        },
        locations:{
          location:[
            {_name: "staging", _path: ""},
            {_name: "temp", _path: ""},
            {_name: "working", _path: ""}
          ]
        },
        ACL: {
          _owner: "",
          _group: "",
          _permission: ""
        },
        properties: {
          property: [
            { _name: "", _value: ""}
          ]
        },
        _xmlns:"uri:falcon:cluster:0.1",
        _name:"",
        _description:"",
        _colo:""
      }
    };

    EntityModel.feedModel = {
      feed: {
        tags: "",
        groups: "",
        frequency: "",
        /*timezone: "GMT+00:00",*/
        "late-arrival": {
          "_cut-off": ""
        },
        clusters: [{
          "cluster": {
            validity: {
              _start: "",
              _end: ""
            },
            retention: {
              _limit: "",
              _action: ""
            },
            _name: "",
            _type: "source"
          }
        }],
        locations: {
          location: [{
            _type: "data",
            _path: "/none"
          }, {
            _type: "stats",
            _path: "/none"
          }, {
            _type: "meta",
            _path: "/none"
          }]
        },
        ACL: {
          _owner: "",
          _group: "",
          _permission: ""
        },
        schema: {
          _location: "/none",
          _provider: "none"
        },
        _xmlns: "uri:falcon:feed:0.1",
        _name: "",
        _description: ""
      }
    };

    EntityModel.datasetModel = {
      toImportModel: undefined,
      UIModel: {
        name: "",
        tags: {
          newTag: { value:"", key:"" },
          tagsArray: [{ value:"_falcon_mirroring_type", key:"HDFS" }],
          tagsString: ""
        },
        formType: "HDFS",
        runOn: "source",
        source: {
          location: "HDFS",
          cluster: "",
          url: "",
          path: "",
          hiveDatabaseType: "databases",
          hiveDatabases: "",
          hiveDatabase: "",
          hiveTables: ""
        },
        target: {
          location: "HDFS",
          cluster: "",
          url: "",
          path: ""
        },
        alerts: {
          alert: { email: "" },
          alertsArray: []
        },
        validity: {
          start: (function () { var d = new Date(); d.setHours(0); d.setMinutes(0); d.setSeconds(0); return d; }()),
          startTime: new Date(),
          end: "",
          endTime: new Date(),
          tz: "GMT+00:00",
          startISO: "",
          endISO: ""
        },
        frequency: {
          number: 5,
          unit: 'minutes'
        },
        allocation: {
          hdfs:{
            maxMaps: 5,
            maxBandwidth: 100
          },
          hive:{
            maxMapsDistcp: 1,
            maxMapsMirror: 5,
            maxMapsEvents: -1,
            maxBandwidth: 100
          }
        },
        hiveOptions: {
          source:{
            stagingPath: "",
            hiveServerToEndpoint: ""
          },
          target:{
            stagingPath: "",
            hiveServerToEndpoint: ""
          }
        },
        retry: {
          policy:"periodic",
          delay: {
            unit: "minutes",
            number: 30
          },
          attempts: 3
        },
        acl: {
          owner: userName,
          group: "users",
          permissions: "0x755"
        }
      },
      HDFS: {
        process: {
          tags: "",
          clusters: {
            cluster: [{
              validity: {
                _start: "2015-03-13T00:00Z",
                _end: "2016-12-30T00:00Z"
              },
              _name: "primaryCluster"
            }]
          },
          parallel: "1",
          order: "LAST_ONLY",
          frequency: "minutes(5)",
          timezone: "UTC",
          properties: {
            property: [
              {
                _name: "oozie.wf.subworkflow.classpath.inheritance",
                _value: "true"
              },
              {
                _name: "distcpMaxMaps",
                _value: "5"
              },
              {
                _name: "distcpMapBandwidth",
                _value: "100"
              },
              {
                _name: "drSourceDir",
                _value: "/user/hrt_qa/dr/test/srcCluster/input"
              },
              {
                _name: "drTargetDir",
                _value: "/user/hrt_qa/dr/test/targetCluster/input"
              },
              {
                _name: "drTargetClusterFS",
                _value: "hdfs://240.0.0.10:8020"
              },
              {
                _name: "drSourceClusterFS",
                _value: "hdfs://240.0.0.10:8020"
              },
              {
                _name: "drNotificationReceivers",
                _value: "NA"
              },
              {
                _name: "targetCluster",
                _value: ""
              },
              {
                _name: "sourceCluster",
                _value: ""
              }
            ]
          },
          workflow: {
            _name: "hdfs-dr-workflow",
            _engine: "oozie",
            _path: "/apps/data-mirroring/workflows/hdfs-replication-workflow.xml",
            _lib: ""
          },
          retry: {
            _policy: "periodic",
            _delay: "minutes(30)",
            _attempts: "3"
          },
          ACL: {
            _owner: "hrt_qa",
            _group: "users",
            _permission: "0x755"
          },
          _xmlns: "uri:falcon:process:0.1",
          _name: "hdfs-replication-adtech"
        }
      },
      HIVE: {
        process: {
          tags: "",
          clusters: {
            cluster: [{
              validity: {
                _start: "2015-03-14T00:00Z",
                _end: "2016-12-30T00:00Z"
              },
              _name: "primaryCluster"
            }]
          },
          parallel: "1",
          order: "LAST_ONLY",
          frequency: "minutes(3)",
          timezone: "UTC",
          properties: {
            property: [
              {
                _name: "oozie.wf.subworkflow.classpath.inheritance",
                _value: "true"
              },
              {
                _name: "distcpMaxMaps",
                _value: "1"
              },
              {
                _name: "distcpMapBandwidth",
                _value: "100"
              },
              {
                _name: "targetCluster",
                _value: "backupCluster"
              },
              {
                _name: "sourceCluster",
                _value: "primaryCluster"
              },
              {
                _name: "targetHiveServer2Uri",
                _value: "hive2://240.0.0.11:10000"
              },
              {
                _name: "sourceHiveServer2Uri",
                _value: "hive2://240.0.0.10:10000"
              },
              {
                _name: "sourceStagingPath",
                _value: "/apps/falcon/primaryCluster/staging"
              },
              {
                _name: "targetStagingPath",
                _value: "/apps/falcon/backupCluster/staging"
              },
              {
                _name: "targetNN",
                _value: "hdfs://240.0.0.11:8020"
              },
              {
                _name: "sourceNN",
                _value: "hdfs://240.0.0.10:8020"
              },
              {
                _name: "sourceServicePrincipal",
                _value: "hive"
              },
              {
                _name: "targetServicePrincipal",
                _value: "hive"
              },
              {
                _name: "targetMetastoreUri",
                _value: "thrift://240.0.0.11:9083"
              },
              {
                _name: "sourceMetastoreUri",
                _value: "thrift://240.0.0.10:9083"
              },
              {
                _name: "sourceTable",
                _value: ""
              },
              {
                _name: "sourceDatabase",
                _value: ""
              },
              {
                _name: "maxEvents",
                _value: "-1"
              },
              {
                _name: "replicationMaxMaps",
                _value: "5"
              },
              {
                _name: "clusterForJobRun",
                _value: "primaryCluster"
              },
              {
                _name: "clusterForJobRunWriteEP",
                _value: "hdfs://240.0.0.10:8020"
              },
              {
                _name: "drJobName",
                _value: "hive-disaster-recovery-sowmya-1"
              },
              {
                _name: "drNotificationReceivers",
                _value: "NA"
              }
            ]
          },
          workflow: {
            _name: "falcon-dr-hive-workflow",
            _engine: "oozie",
            _path: "/apps/data-mirroring/workflows/hive-disaster-recovery-workflow.xml",
            _lib: ""
          },
          retry: {
            _policy: "periodic",
            _delay: "minutes(30)",
            _attempts: "3"
          },
          ACL: {
            _owner: "hrt_qa",
            _group: "users",
            _permission: "0x755"
          },
          _xmlns: "uri:falcon:process:0.1",
          _name: "hive-disaster-recovery-sowmya-1"
        }
      }

    };


    /*setTimeout(function () {


      var xmlStrDatasetHDFS ='<?xml version="1.0" encoding="UTF-8" standalone="yes"?><process xmlns="uri:falcon:process:0.1" name="hdfs-replication-adtech"><tags></tags><clusters><cluster name="primaryCluster"><validity start="2015-03-13T00:00Z" end="2016-12-30T00:00Z"/></cluster></clusters><parallel>1</parallel><order>LAST_ONLY</order><frequency>minutes(5)</frequency><timezone>UTC</timezone><properties><property name="oozie.wf.subworkflow.classpath.inheritance" value="true"/><property name="distcpMaxMaps" value="5"/><property name="distcpMapBandwidth" value="100"/><property name="drSourceDir" value="/user/hrt_qa/dr/test/srcCluster/input"/><property name="drTargetDir" value="/user/hrt_qa/dr/test/targetCluster/input"/><property name="drTargetClusterFS" value="hdfs://240.0.0.10:8020"/><property name="drSourceClusterFS" value="hdfs://240.0.0.10:8020"/></properties><workflow name="hdfs-dr-workflow" engine="oozie" path="hdfs://node-1.example.com:8020/apps/falcon/recipe/hdfs-replication/resources/runtime/hdfs-replication-workflow.xml" lib=""/><retry policy="periodic" delay="minutes(30)" attempts="3"/><ACL owner="hrt_qa" group="users" permission="0x755"/></process>';
      var xmlStrDatasetHIVE = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><process xmlns="uri:falcon:process:0.1" name="hive-disaster-recovery-sowmya-1"><tags></tags><clusters><cluster name="primaryCluster"><validity start="2015-03-14T00:00Z" end="2016-12-30T00:00Z"/></cluster></clusters><parallel>1</parallel><order>LAST_ONLY</order><frequency>minutes(3)</frequency><timezone>UTC</timezone><properties><property name="oozie.wf.subworkflow.classpath.inheritance" value="true"/><property name="distcpMaxMaps" value="1"/><property name="distcpMapBandwidth" value="100"/><property name="targetCluster" value="backupCluster"/><property name="sourceCluster" value="primaryCluster"/><property name="targetHiveServer2Uri" value="hive2://240.0.0.11:10000"/><property name="sourceHiveServer2Uri" value="hive2://240.0.0.10:10000"/><property name="sourceStagingPath" value="/apps/falcon/primaryCluster/staging"/><property name="targetStagingPath" value="/apps/falcon/backupCluster/staging"/><property name="targetNN" value="hdfs://240.0.0.11:8020"/><property name="sourceNN" value="hdfs://240.0.0.10:8020"/><property name="sourceServicePrincipal" value="hive"/><property name="targetServicePrincipal" value="hive"/><property name="targetMetastoreUri" value="thrift://240.0.0.11:9083"/><property name="sourceMetastoreUri" value="thrift://240.0.0.10:9083"/><property name="sourceTable" value="testtable_dr"/><property name="sourceDatabase" value="default"/><property name="sourceDatabase" value="db1, db2, db3"/><property name="sourceTable" value="*"/><property name="maxEvents" value="-1"/><property name="replicationMaxMaps" value="5"/><property name="clusterForJobRun" value="primaryCluster"/><property name="clusterForJobRunWriteEP" value="hdfs://240.0.0.10:8020"/><property name="drJobName" value="hive-disaster-recovery-sowmya-1"/></properties><workflow name="falcon-dr-hive-workflow" engine="oozie" path="hdfs://node-1.example.com:8020/apps/falcon/recipe/hive-disaster-recovery/resources/runtime/hive-disaster-recovery-workflow.xml" lib=""/><retry policy="periodic" delay="minutes(30)" attempts="3"/><ACL owner="hrt_qa" group="users" permission="0x755"/></process>';


      var modelResulting = X2jsService.xml_str2json(xmlStrDatasetHIVE);

      console.log(modelResulting);
      console.log(JSON.stringify(modelResulting));

    }, 3000);*/



    return EntityModel;

  }]);

})();






