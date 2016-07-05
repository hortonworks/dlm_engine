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
   * @name app.controllers.process.ProcessAdvancedCtrl
   * @requires EntityModel the entity model to copy the process entity from
   * @requires Falcon the falcon entity service
   */
  var processModule = angular.module('app.controllers.process');

  processModule.controller('ProcessAdvancedCtrl', ['$scope', 'EntityFactory', '$timeout', 'DateHelper',
                                              function($scope, entityFactory, $timeout, DateHelper) {

    $scope.init = function() {
      $scope.dateFormat = DateHelper.getLocaleDateFormat();
    };

    $scope.openDatePicker = function($event, container) {
      $event.preventDefault();
      $event.stopPropagation();
      container.opened = true;
    };

    $scope.validateStartEndDate = function () {
      delete $scope.invalidEndDate;
      if (this.input.start && this.input.end) {
        var startDate = new Date(this.input.start),
          endDate = new Date(this.input.end);
        if (endDate.toString !== 'Invalid Date' && startDate.toString !== 'Invalid Date') {
          if (startDate > endDate) {
            $scope.invalidEndDate = "ng-dirty ng-invalid";
          }
        }
      }
    };

    $scope.init();

    //-----------PROPERTIES----------------//
    $scope.addProperty = function () {
      var lastOne = $scope.process.properties.length - 1;
      if($scope.process.properties[lastOne].name && $scope.process.properties[lastOne].value) {
        $scope.process.properties.push(entityFactory.newProperty("", ""));
      }
    };
    $scope.removeProperty = function(index) {
      if(index !== null && $scope.process.properties[index]) {
        $scope.process.properties.splice(index, 1);
      }
    };

    $scope.policyChange = function(){
      if($scope.process.retry.policy === 'final'){
       $scope.process.retry.delay.quantity = '0';
       $scope.process.retry.delay.unit = 'minutes';
       $scope.process.retry.attempts = '0';
      }else{
        $scope.process.retry.delay.quantity = '30';
        $scope.process.retry.delay.unit = 'minutes';
        $scope.process.retry.attempts = '3';
      }
    }

  }]);

})();
