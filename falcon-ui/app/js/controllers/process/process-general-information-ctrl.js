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

  /***
   * @ngdoc controller
   * @name app.controllers.feed.FeedController
   * @requires clusters the list of clusters to display for selection of source
   * @requires EntityModel the entity model to copy the feed entity from
   * @requires Falcon the falcon entity service
   */
  var feedModule = angular.module('app.controllers.process');

  feedModule.controller('ProcessGeneralInformationCtrl', [ '$scope', 'clustersList', 'feedsList', 'EntityFactory',
    function($scope, clustersList, feedsList, entityFactory) {

    $scope.nameValid = false;

    $scope.init = function() {
      unwrapClusters(clustersList);
      unwrapFeeds(feedsList);
    };

    // TAGS
    $scope.addTag = function() {
      $scope.process.tags.push({key: null, value: null});
    };

    $scope.removeTag = function(index) {
      if(index >= 0 && $scope.process.tags.length > 1) {
        $scope.process.tags.splice(index, 1);
      }
    };

    // inputs
    $scope.addInput = function () {
      $scope.process.inputs.push(entityFactory.newInput());
    };

    $scope.removeInput = function (index) {
      if (index >= 0) {
        $scope.process.inputs.splice(index, 1);
      }
    };

    // OUTPUTS
    $scope.addOutput = function () {
      $scope.process.outputs.push(entityFactory.newOutput());
    };

    $scope.removeOutput = function (index) {
      if (index >= 0) {
        $scope.process.outputs.splice(index, 1);
      }
    };

    $scope.init();

    function unwrapClusters(clusters) {
      $scope.clusterList = [];
      var typeOfData = Object.prototype.toString.call(clusters.entity);
      if(typeOfData === "[object Array]") {
        $scope.clusterList = clusters.entity;
      } else if(typeOfData === "[object Object]") {
        $scope.clusterList = [clusters.entity];
      } else {
        //console.log("type of data not recognized");
      }
    }

    function unwrapFeeds(feeds) {
      $scope.feedsList = [];
      var typeOfData = Object.prototype.toString.call(feeds.entity);
      if (typeOfData === "[object Array]") {
        $scope.feedsList = feeds.entity;
      } else if (typeOfData === "[object Object]") {
        $scope.feedsList = [feeds.entity];
      } else {
        //console.log("type of data not recognized");
      }
    }

  }]);


})();
