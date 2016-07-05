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
   * @name app.controllers.datasource.DatasourceGeneralInformationController
   * @requires EntityModel the entity model to copy the datasource entity from
   * @requires Falcon the falcon entity service
   */
  var datasourceModule = angular.module('app.controllers.datasource');

  datasourceModule.controller('DatasourceGeneralInformationController', [ "$scope", function ($scope) {
    $scope.addTag = function () {
      $scope.datasource.tags.push({key: null, value: null});
    };

    $scope.removeTag = function (index) {
      if (index >= 0 && $scope.datasource.tags.length > 1) {
        $scope.datasource.tags.splice(index, 1);
      }
    };

    $scope.addParameter = function () {
      $scope.datasource.parameters.push({name: null, value: null});
    };

    $scope.removeParameter = function (index) {
      if (index >= 0 && $scope.datasource.parameters.length > 0) {
        $scope.datasource.parameters.splice(index, 1);
      }
    };

    $scope.addProperty = function () {
      $scope.datasource.customProperties.push({name: null, value: null});
    };

    $scope.removeProperty = function (index) {
      if (index >= 0 && $scope.datasource.customProperties.length > 0) {
        $scope.datasource.customProperties.splice(index, 1);
      }
    };

    $scope.addDriverJar = function () {
      var lastOne = $scope.datasource.driver.jar.length - 1;
      if($scope.datasource.driver.jar[lastOne].value) {
        $scope.datasource.driver.jar.push({value:""});
      }
    };

    $scope.removeDriverJar = function(index) {
      if(index !== null && $scope.datasource.driver.jar[index]) {
        $scope.datasource.driver.jar.splice(index, 1);
      }
    };

    $scope.getDatabaseDefaultDetails = function() {
      switch ($scope.datasource.type) {
        // case "sqlserver":
        //   $scope.datasource.port = 1433;
        //   $scope.datasource.driver.clazz = "net.sourceforge.jtds.jdbc.Driver";
        //   return;
        case "mysql":
          $scope.datasource.interfaces.interfaces[0].endpoint = "jdbc:mysql://db_host:3306";
          $scope.datasource.driver.clazz = "com.mysql.jdbc.Driver";
          return;
        case "hsql":
          $scope.datasource.interfaces.interfaces[0].endpoint = "jdbc:hsqldb:hsql://db_host:9001";
          $scope.datasource.driver.clazz = "org.hsqldb.jdbcDriver";
          return;
        case "oracle":
          $scope.datasource.interfaces.interfaces[0].endpoint = "jdbc:oracle:thin@db_host:1526:oracle_sid";
          $scope.datasource.driver.clazz = "oracle.jdbc.driver.OracleDriver";
          return;
        // case "teradata":
        //   $scope.datasource.port = 1025;
        //   $scope.datasource.driver.clazz = "com.ncr.teradata.TeraDriver";
        //   return;
        // case "db2":
        //   $scope.datasource.port = 50000;
        //   $scope.datasource.driver.clazz = "com.ibm.db2.jcc.DB2Driver";
        //   return;
        default:
          $scope.datasource.interfaces.interfaces[0].endpoint = "jdbc:"
          $scope.datasource.driver.clazz = "org.apache.sqoop.connector.jdbc.GenericJdbcConnector";
          return;
      }
    }

  //   $scope.datasource.interfaces.interfaces[0].endpoint = $scope.datasource.type;

  //   $scope.$watchCollection(
  //     "[datasource.type, datasource.host, datasource.port, datasource.databaseName]", function() {
  //     var connectionString;
  //     switch ($scope.datasource.type) {
  //       case "sqlserver":
  //         connectionString = "jdbc:jtds:sqlserver://";
  //       case "mysql":
  //         connectionString = "jdbc:mysql://";
  //       case "postgresql":
  //         connectionString = "jdbc:postgresql://";
  //       case "oracle":
  //         connectionString = "jdbc:oracle:thin:@//";
  //       case "teradata":
  //         connectionString = "jdbc:teradata://";
  //       case "db2":
  //         connectionString = "jdbc:db2://";
  //       default:
  //         connectionString = "jdbc://";
  //     };
  //     $scope.datasource.interfaces.interfaces[0].endpoint = connectionString
  //         + $scope.datasource.host + ":"
  //         + $scope.datasource.port + "/"
  //         + $scope.datasource.databaseName;
  //
  //   });
  //
  }]);


}());
