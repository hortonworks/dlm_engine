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

  var navHeaderModule = angular.module('app.controllers.navHeader', [
    'app.services.entity.model',
    'app.services.validation',
    'ngCookies'
  ]);

  navHeaderModule.controller('HeaderController', [
    '$rootScope', '$scope', '$state', '$cookieStore', 'EntityModel', 'ValidationService',
    function ($rootScope, $scope, $state, $cookieStore, EntityModel, validationService) {

      $scope.fake = { focus: false }; //used in upload button to fake the focus borders

      $scope.isInForm = function (type) {
        if($rootScope.currentState) {
          var currState = $rootScope.currentState.split('.'),
            formType = currState[1];
          return type === formType;
        }
      };

      $scope.resetCluster = function () {
        validationService.displayValidations = {show: false, nameShow: false};
        EntityModel.clusterModel = { cluster: { tags: "", interfaces: { interface: [
            { _type: "readonly", _endpoint: "hftp://sandbox.hortonworks.com:50070", _version: "2.2.0"},
            { _type: "write", _endpoint: "hdfs://sandbox.hortonworks.com:8020", _version: "2.2.0"},
            { _type: "execute", _endpoint: "sandbox.hortonworks.com:8050", _version: "2.2.0"},
            { _type: "workflow", _endpoint: "http://sandbox.hortonworks.com:11000/oozie/", _version: "4.0.0"},
            { _type: "messaging", _endpoint: "tcp://sandbox.hortonworks.com:61616?daemon=true", _version: "5.1.6"}
          ]}, locations: { location: [{ _name: "staging", _path: ""}, { _name: "temp", _path: ""}, { _name: "working", _path: ""}]},
          ACL: { _owner: "", _group: "", _permission: ""}, properties: { property: [{ _name: "", _value: ""}]},
          _xmlns: "uri:falcon:cluster:0.1", _name: "", _description: "", _colo: ""}};
        $state.go("forms.cluster.general");
      };

      $scope.resetProcess = function () {
        validationService.displayValidations = {show: false, nameShow: false};
        $scope.cloningMode = true;
        $scope.models.processModel = null;
        $state.go("forms.process.general");
      };

      $scope.resetFeed = function () {
        validationService.displayValidations = {show: false, nameShow: false};
        $scope.cloningMode = true;
        $scope.models.feedModel = null;
        $state.go("forms.feed.general");
      };

      $scope.resetDataset = function () {
        validationService.displayValidations = {show: false, nameShow: false};
        EntityModel.datasetModel.toImportModel = undefined;
        EntityModel.datasetModel.UIModel={name:"",tags:{newTag:{value:"",key:""},tagsArray:[{ key:"_falcon_mirroring_type", value:"HDFS" }],tagsString:""},formType:"HDFS",runOn:"source",source:{location:"HDFS",cluster:"",url:"",path:"",hiveDatabaseType:"databases",hiveDatabases:"",hiveDatabase:"",hiveTables:""},target:{location:"HDFS",cluster:"",url:"",path:""},alerts:{alert:{email:""},alertsArray:[]},validity:{start:(function () { var d = new Date(); d.setHours(0); d.setMinutes(0); d.setSeconds(0); return d; }()),startTime:new Date(),end:"",endTime:new Date(),tz:"GMT+00:00",startISO:"",endISO:""},frequency:{number:5,unit:"minutes"},allocation:{hdfs:{maxMaps:5,maxBandwidth:100},hive:{maxMapsDistcp:1,maxMapsMirror:5,maxMapsEvents:-1,maxBandwidth:100}},hiveOptions:{source:{stagingPath:"",hiveServerToEndpoint:""},target:{stagingPath:"",hiveServerToEndpoint:""}},retry:{policy:"periodic",delay:{unit:"minutes",number:30},attempts:3},acl:{owner:$cookieStore.get("userToken").user,group:"users",permissions:"0x755"}};
        $scope.cloningMode = true;
        $scope.models.feedModel = null;
        $state.go("forms.dataset.general");
      };

      $scope.userLogged = function () {
    	  if($rootScope.userLogged()){
    	  	if(angular.isDefined($cookieStore.get('userToken')) && $cookieStore.get('userToken') !== null){
    	  		$scope.userToken = $cookieStore.get('userToken').user;
    	  		return true;
    	  	}else{
    	  		$timeout(function() {
    	  			$scope.userToken = $cookieStore.get('userToken').user;
    	  			return true;
    	  		}, 1000);
    	  	}
    	  }else{
    		  return false;
    	  }
      };

      $scope.logOut = function() {
      	$cookieStore.put('userToken', null);
      	$state.transitionTo('login');
      };

    }]);

})();
