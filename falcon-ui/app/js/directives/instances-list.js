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

	var entitiesListModule = angular.module('app.directives.instances-list', ['app.services' ]);

  entitiesListModule.controller('InstancesListCtrl', ['$scope', 'Falcon', 'X2jsService', '$window', 'EncodeService',
                                      function($scope, Falcon, X2jsService, $window, encodeService) {

    //$scope.downloadEntity = function(logURL) {
    //  Falcon.logRequest();
    //  Falcon.getInstanceLog(logURL) .success(function (data) {
    //    Falcon.logResponse('success', data, false, true);
    //    $window.location.href = 'data:application/octet-stream,' + encodeService.encode(data);
    //  }).error(function (err) {
    //    Falcon.logResponse('error', err, false);
    //  });
    //};

    $scope.downloadEntity = function(logURL) {
      $window.location.href = logURL;
    };

  }]);

  entitiesListModule.filter('tagFilter', function () {
    return function (items) {
      var filtered = [], i;
      for (i = 0; i < items.length; i++) {
        var item = items[i];
        if(!item.list || !item.list.tag) { item.list = {tag:[""]}; }
        filtered.push(item);
      }
      return filtered;
    };
  });

  entitiesListModule.directive('instancesList', ["$timeout", 'Falcon', '$filter', function($timeout, Falcon, $filter) {
    return {
      scope: {
        input: "=",
        schedule: "=",
        resume:"=",
        rerun:"=",
        suspend: "=",
        stop: "=",
        type: "=",
        name: "=",
        start: "=",
        end: "=",
        instanceDetails:"=",
        refresh: "=",
        pages: "=",
        nextPages: "=",
        prevPages: "=",
        goPage: "=",
        changePagesSet: "="
      },
      controller: 'InstancesListCtrl',
      restrict: "EA",
      templateUrl: 'html/directives/instancesListDv.html',
      link: function (scope) {
        scope.server = Falcon;
        scope.$watch('input', function() {
          scope.selectedRows = [];
          scope.checkButtonsToShow();

        }, true);

        var resultsPerPage = 10;
        var visiblePages = 3;
        scope.selectedRows = [];
        scope.$parent.refreshInstanceList(scope.type, scope.name, scope.start, scope.end);

        scope.startSortOrder = "desc";
        scope.endSortOrder = "desc";
        scope.statusSortOrder = "desc";

        scope.checkedRow = function (name) {
          var isInArray = false;
          scope.selectedRows.forEach(function(item) {
            if (name === item.instance) {
              isInArray = true;
            }
          });
          return isInArray;
        };

        scope.simpleFilter = {};

        scope.selectedDisabledButtons = {
          schedule:true,
          suspend:true,
          resume:true,
          stop:true
        };

        scope.checkButtonsToShow = function() {
          var statusCount = {
            "SUBMITTED":0,
            "RUNNING":0,
            "SUSPENDED":0,
            "UNKNOWN":0,
            "KILLED":0,
            "WAITING":0,
            "FAILED":0,
            "SUCCEEDED":0
          };

          $timeout(function() {

            if(scope.selectedRows.length === scope.input.length){
              scope.selectedAll = true;
            }else{
              scope.selectedAll = false;
            }

            scope.selectedRows.forEach(function(instance) {
              statusCount[instance.status] = statusCount[instance.status]+1;
            });

            if(statusCount.SUBMITTED > 0) {
              if(statusCount.RUNNING > 0 || statusCount.SUSPENDED > 0 || statusCount.UNKNOWN > 0 || statusCount.KILLED > 0 || statusCount.WAITING > 0 || statusCount.FAILED > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:false, suspend:true, resume:true, stop:true };
              }
            }
            if(statusCount.RUNNING > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.SUSPENDED > 0 || statusCount.UNKNOWN > 0 || statusCount.KILLED > 0 || statusCount.WAITING > 0 || statusCount.FAILED > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:false };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:false, resume:true, stop:true  };
              }
            }
            if (statusCount.SUSPENDED > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.RUNNING > 0 || statusCount.UNKNOWN > 0 || statusCount.KILLED > 0 || statusCount.WAITING > 0 || statusCount.FAILED > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:false };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:false, stop:false };
              }
            }
            if (statusCount.KILLED > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.SUSPENDED > 0 || statusCount.RUNNING > 0 || statusCount.UNKNOWN > 0 || statusCount.WAITING > 0 || statusCount.FAILED > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:false, stop:true };
              }
            }
            if(statusCount.WAITING > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.RUNNING > 0 || statusCount.SUSPENDED > 0 || statusCount.UNKNOWN > 0 || statusCount.KILLED > 0 || statusCount.FAILED > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true  };
              }
            }
            if (statusCount.FAILED > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.SUSPENDED > 0 || statusCount.RUNNING > 0 || statusCount.UNKNOWN > 0 || statusCount.KILLED > 0 || statusCount.WAITING > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true };
              }
            }
            if(statusCount.SUCCEEDED > 0) {
              if(statusCount.SUBMITTED > 0 || statusCount.RUNNING > 0 || statusCount.SUSPENDED > 0 || statusCount.UNKNOWN > 0 || statusCount.KILLED > 0 || statusCount.WAITING > 0 || statusCount.FAILED > 0) {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true };
              }
              else {
                scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:false, stop:true };
              }
            }
            if (statusCount.UNKNOWN > 0) {
              scope.selectedDisabledButtons = { schedule:true, suspend:true, resume:true, stop:true };
            }

            if(scope.selectedRows.length === 0) {
              scope.selectedDisabledButtons = {
                schedule:true,
                resume:true,
                suspend:true,
                stop:true
              };
            }
          }, 50);
        };

        var isSelected = function(item){
          var selected = false;
          scope.selectedRows.forEach(function(entity) {
            if(angular.equals(item, entity)){
              selected = true;
            }
          });
          return selected;
        }

        scope.checkAll = function () {
          if(scope.selectedRows.length >= scope.input.length){
            angular.forEach(scope.input, function (item) {
              scope.selectedRows.pop();
            });
          }else{
            angular.forEach(scope.input, function (item) {
              var checkbox = {'instance':item.instance, 'startTime':item.startTime, 'endTime':item.endTime, 'status':item.status, 'type':scope.type, 'logFile':item.logFile};
              if(!isSelected(checkbox)){
                scope.selectedRows.push(checkbox);
              }
            });
          }
        };

        scope.goInstanceDetails = function(instance) {
          scope.instanceDetails(instance);
        };

        scope.scopeSuspend = function () {
          for(var i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            var start = scope.selectedRows[i].instance;
            var end = addOneMin(start);
            console.log(start + " *** " + end);
            scope.suspend(scope.type, scope.name, start, end);
          }
        };

        scope.scopeResume = function () {
          for(var i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            var start = scope.selectedRows[i].instance;
            var end = addOneMin(start);
            if(scope.selectedRows[i].status === "KILLED" || scope.selectedRows[i].status === "SUCCEEDED"){
              scope.rerun(scope.type, scope.name, start, end);
            }else{
              scope.resume(scope.type, scope.name, start, end);
            }
          }
        };

        scope.scopeStop = function () {
          for(var i = 0; i < scope.selectedRows.length; i++) {
            var multiRequestType = scope.selectedRows[i].type.toLowerCase();
            Falcon.responses.multiRequest[multiRequestType] += 1;
            var start = scope.selectedRows[i].instance;
            var end = addOneMin(start);
            scope.stop(scope.type, scope.name, start, end);
          }
        };

        scope.download = function() {
          var i;
          for(i = 0; i < scope.selectedRows.length; i++) {
            scope.downloadEntity(scope.selectedRows[i].logFile);
          }
        };

        scope.scopeGoPage = function (page) {
          scope.goPage(page);
        };

        scope.scopeNextOffset = function (page) {
          var offset = (parseInt(scope.pages[0].label)+(visiblePages-1))*resultsPerPage;
          scope.changePagesSet(offset, page, 0, scope.start, scope.end);
        };

        scope.scopePrevOffset = function (page) {
          var offset = (parseInt(scope.pages[0].label)-(visiblePages+1))*resultsPerPage;
          scope.changePagesSet(offset, page, visiblePages-1, scope.start, scope.end);
        };

        scope.filterInstances = function(orderBy){
          var sortOrder = "";
          if(orderBy !== undefined && orderBy !== ""){
            if(orderBy === "startTime"){
              if(scope.startSortOrder === "desc"){
                scope.startSortOrder = "asc";
              }else{
                scope.startSortOrder = "desc";
              }
              sortOrder = scope.startSortOrder;
            }else if(orderBy === "endTime"){
              if(scope.endSortOrder === "desc"){
                scope.endSortOrder = "asc";
              }else{
                scope.endSortOrder = "desc";
              }
              sortOrder = scope.endSortOrder;
            }else if(orderBy === "status"){
              if(scope.statusSortOrder === "desc"){
                scope.statusSortOrder = "asc";
              }else{
                scope.statusSortOrder = "desc";
              }
              sortOrder = scope.statusSortOrder;
            }
          }else{
            orderBy = "startTime";
            sortOrder = "desc";
          }
          var start = "";
          if(scope.startFilter !== undefined && scope.startFilter !== ""){
            start = $filter('date')(scope.startFilter, "yyyy-MM-ddTHH:mm:ssZ");
          }
          var end = "";
          if(scope.endFilter !== undefined && scope.endFilter !== ""){
            end = $filter('date')(scope.endFilter, "yyyy-MM-ddTHH:mm:ssZ");
          }
          scope.$parent.refreshInstanceList(scope.type, scope.name, start, end, scope.statusFilter, orderBy, sortOrder);
        }

        var addOneMin = function(time){
          var newtime = parseInt(time.substring(time.length-3, time.length-1));
          if(newtime === 59){
            newtime = 0;
          }else{
            newtime++;
          }
          if(newtime < 10){
            newtime = "0"+newtime;
          }
          return time.substring(0, time.length-3) + newtime + "Z";
        }

      }
    };
  }]);

})();