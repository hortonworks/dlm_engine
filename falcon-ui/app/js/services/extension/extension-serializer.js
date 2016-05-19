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
  var module = angular.module('app.services.extension.serializer',
    ['app.services',
      'app.services.entity.factory',
      'app.services.entity.model']);

  module.factory('ExtensionSerializer', ['EntityFactory', 'DateHelper', 'EntityModel',
    function(EntityFactory, DateHelper, EntityModel) {

      var convertTags = function (tagsArray) {
        var result = [];
        tagsArray.forEach(function(element) {
          if(element.key && element.value) {
            result.push(element.key + "=" + element.value);
          }
        });
        result = result.join(",");
        return result;
      };

      var convertObjectToString = function (obj) {
        var str = '';
        for (var key in obj) {
          if (obj.hasOwnProperty(key)) {
              str += key + '=' + obj[key] + '\n';
          }
        }
        return str;
      };

      var serializeSnapshotExtensionProperties = function(snapshot) {
        var snapshotProps = {};
        snapshotProps.jobName = snapshot.name;
        snapshotProps.jobValidityStart = DateHelper.createISO(snapshot.validity.start.date,
          snapshot.validity.start.time, snapshot.validity.timezone);
        snapshotProps.jobValidityEnd = DateHelper.createISO(snapshot.validity.end.date,
          snapshot.validity.end.time, snapshot.validity.timezone);
        snapshotProps.jobFrequency = snapshot.frequency.unit + '(' + snapshot.frequency.quantity + ')';
        snapshotProps.jobTimezone = snapshot.validity.timezone;
        snapshotProps.jobTags = convertTags(snapshot.tags);
        snapshotProps.jobRetryPolicy = snapshot.retry.policy;
        snapshotProps.jobRetryDelay = snapshot.retry.delay.unit + '(' + snapshot.retry.delay.quantity + ')';
        snapshotProps.jobRetryAttempts = snapshot.retry.attempts;
        snapshotProps.jobAclOwner = snapshot.ACL.owner;
        snapshotProps.jobAclGroup = snapshot.ACL.group;
        snapshotProps.jobAclPermission = snapshot.ACL.permission;

        snapshotProps.sourceCluster = snapshot.source.cluster;
        snapshotProps.sourceSnapshotDir = snapshot.source.directoryPath.trim();
        snapshotProps.targetCluster = snapshot.target.cluster;
        snapshotProps.targetSnapshotDir = snapshot.target.directoryPath.trim();
        if (snapshot.runOn === 'source') {
          snapshotProps.jobClusterName = snapshot.source.cluster;
        } else if (snapshot.runOn === 'target') {
          snapshotProps.jobClusterName = snapshot.target.cluster;
        }
        snapshotProps.sourceSnapshotRetentionAgeLimit = snapshot.source.deleteFrequency.unit
          + '(' + snapshot.source.deleteFrequency.quantity + ')';
        snapshotProps.targetSnapshotRetentionAgeLimit = snapshot.target.deleteFrequency.unit
          + '(' + snapshot.target.deleteFrequency.quantity + ')';
        snapshotProps.sourceSnapshotRetentionNumber = snapshot.source.retentionNumber;
        snapshotProps.targetSnapshotRetentionNumber = snapshot.target.retentionNumber;
        if (snapshot.allocation && snapshot.allocation.distcpMaxMaps) {
          snapshotProps.distcpMaxMaps = snapshot.allocation.distcpMaxMaps;
        }
        if (snapshot.allocation && snapshot.allocation.distcpMapBandwidth) {
          snapshotProps.distcpMapBandwidth = snapshot.allocation.distcpMapBandwidth;
        }
        snapshotProps.tdeEncryptionEnabled = snapshot.tdeEncryptionEnabled;
        if (snapshot.alerts.length > 0) {
          snapshotProps.jobNotificationType = 'email';
          snapshotProps.jobNotificationReceivers = snapshot.alerts.join();
        }
        return snapshotProps;
      };

      var serializeSnapshotExtensionModel = function(model) {
        var snapshotObj = EntityFactory.newEntity('snapshot');
        snapshotObj.name = model.process._name;

        snapshotObj.retry.policy = model.process.retry._policy;
        snapshotObj.retry.attempts = model.process.retry._attempts;
        snapshotObj.retry.delay.number = (function () {
          return parseInt(model.process.retry._delay.split('(')[1]);
        }());
        snapshotObj.retry.delay.unit = (function () {
          return model.process.retry._delay.split('(')[0];
        }());

        snapshotObj.frequency.number = (function () {
          return parseInt(model.process.frequency.split('(')[1]);
        }());
        snapshotObj.frequency.unit = (function () {
          return model.process.frequency.split('(')[0];
        }());

        // snapshotObj.ACL.owner = model.process.ACL._owner;
        // snapshotObj.ACL.group = model.process.ACL._group;
        // snapshotObj.ACL.permissions = model.process.ACL._permission;

        snapshotObj.validity.timezone = model.process.timezone;
        snapshotObj.validity.start.date = DateHelper.importDate (model.process.clusters.cluster[0].validity._start, model.process.timezone);
        snapshotObj.validity.start.time = DateHelper.importDate (model.process.clusters.cluster[0].validity._start, model.process.timezone);
        snapshotObj.validity.end.date = DateHelper.importDate (model.process.clusters.cluster[0].validity._end, model.process.timezone);
        snapshotObj.validity.end.time = DateHelper.importDate (model.process.clusters.cluster[0].validity._end, model.process.timezone);

        snapshotObj.tags = (function () {
          var array = [];
          if(model.process && model.process.tags){
            model.process.tags.split(',').forEach(function (fieldToSplit) {
              var splittedString = fieldToSplit.split('=');
              if (splittedString[0] != '_falcon_extension_name' && splittedString[0] != '_falcon_extension_job') {
                array.push({key: splittedString[0], value: splittedString[1]});
              }
            });
          }
          return array;
        }());

        if (model.process.notification._to) {
          snapshotObj.alerts = (function () {
            if (model.process.notification._to !== "NA") {
              return model.process.notification._to.split(',');
            } else {
              return [];
            }
          }());
        }

        model.process.properties.property.forEach(function (item) {
            if (item._name === 'distcpMaxMaps') {
              snapshotObj.allocation.distcpMaxMaps = item._value;
            }
            if (item._name === 'distcpMapBandwidth') {
              snapshotObj.allocation.distcpMapBandwidth = item._value;
            }
            if (item._name === 'tdeEncryptionEnabled') {
              snapshotObj.tdeEncryptionEnabled = item._value;
            }
            if (item._name === 'targetCluster') {
              snapshotObj.target.cluster = item._value;
            }
            if (item._name === 'sourceCluster') {
              snapshotObj.source.cluster = item._value;
            }
            if (item._name === 'sourceSnapshotDir') {
              snapshotObj.source.directoryPath = item._value;
            }
            if (item._name === 'targetSnapshotDir') {
              snapshotObj.target.directoryPath = item._value;
            }
            if (item._name === 'sourceSnapshotRetentionNumber') {
              snapshotObj.source.retentionNumber = item._value;
            }
            if (item._name === 'targetSnapshotRetentionNumber') {
              snapshotObj.target.retentionNumber = item._value;
            }

            if (item._name === 'sourceSnapshotRetentionAgeLimit') {
              snapshotObj.source.deleteFrequency.number = (function () {
                return parseInt(item._value.split('(')[1]);
              }());
              snapshotObj.source.deleteFrequency.unit = (function () {
                return item._value.split('(')[0];
              }());
            }

            if (item._name === 'targetSnapshotRetentionAgeLimit') {
              snapshotObj.target.deleteFrequency.number = (function () {
                return parseInt(item._value.split('(')[1]);
              }());
              snapshotObj.target.deleteFrequency.unit = (function () {
                return item._value.split('(')[0];
              }());
            }
          });

          if (snapshotObj.source.cluster === model.process.clusters.cluster[0]._name) {
            snapshotObj.runOn = "source";
          }
          if (snapshotObj.target.cluster === model.process.clusters.cluster[0]._name) {
            snapshotObj.runOn = "target";
          }

          return snapshotObj;
      };

      var serializeExtensionProperties = function(extension, type) {
        if (type === 'snapshot') {
          return serializeSnapshotExtensionProperties(extension);
        }
      };

      var serializeExtensionModel = function(extensionModel, type) {
        if (type === 'snapshot') {
          return serializeSnapshotExtensionModel(extensionModel);
        }
      };

      return {
        serializeExtensionProperties: serializeExtensionProperties,
        serializeExtensionModel: serializeExtensionModel,
        convertObjectToString: convertObjectToString,
        convertTags: convertTags
      };

  }]);
})();
