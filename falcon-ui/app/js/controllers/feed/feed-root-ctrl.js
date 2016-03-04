/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * 'License'); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an 'AS IS' BASIS,
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
  var feedModule = angular.module('app.controllers.feed');

  feedModule.controller('FeedController',
    [ '$scope', '$state', '$timeout', "RouteHelper",
      'Falcon', 'X2jsService',
      'JsonTransformerFactory', 'EntityFactory',
      'EntitySerializer', '$interval',
      '$controller', "ValidationService",
      "SpinnersFlag", "$rootScope", "FeedModel",
      function($scope, $state, $timeout, RouteHelper, Falcon,
               X2jsService, transformerFactory, entityFactory,
               serializer, $interval, $controller,
               validationService, SpinnersFlag, $rootScope, feedModel) {

        $scope.entityType = 'feed';
        var stateMatrix = {
                general : {previous : '', next : 'properties'},
                properties : {previous : 'general', next : 'location'},
                location : {previous : 'properties', next : 'clusters'},
                clusters : {previous : 'location', next : 'summary'},
                summary : {previous : 'clusters', next : ''}
        };
        //extending root controller
        $controller('EntityRootCtrl', {
          $scope: $scope
        });

        $scope.loadOrCreateEntity = function() {
          var type = $scope.entityType;
          if(!feedModel && $scope.$parent.models.feedModel){
            feedModel = $scope.$parent.models.feedModel;
          }
          $scope.$parent.models.feedModel = null;
          return feedModel ? serializer.preDeserialize(feedModel, type) : entityFactory.newEntity(type);
        };

        $scope.init = function() {
          $scope.baseInit();
          var type = $scope.entityType;
          $scope[type] = $scope.loadOrCreateEntity();
          if(feedModel && feedModel.clone === true){
            $scope.cloningMode = true;
            $scope.editingMode = false;
            $scope[type].name = "";
          }else if(feedModel && feedModel.edit === true){
            $scope.editingMode = true;
            $scope.cloningMode = false;
          }else{
            $scope.editingMode = false;
            $scope.cloningMode = false;
          }
          $scope.dateFormat ='MM/dd/yyyy';
        };

        $scope.openDatePicker = function($event, container) {
          $event.preventDefault();
          $event.stopPropagation();
          container.opened = true;
        };

        $scope.init();

        $scope.$watch("feed.storage.fileSystem.locations",function(){
          $scope.feed.clusters.forEach(function (feedCluster) {
            feedCluster.storage.fileSystem.locations.forEach(function (location) {
              if (location.type === 'data') {
                var dataLocation = $scope.feed.storage.fileSystem.locations.filter(function(obj) {
                  return obj.type == 'data';
                })[0];
                location.path = (dataLocation != undefined) ? dataLocation.path : '';
              }
            });
          });
        }, true);

        $scope.transform = function() {
          var type = $scope.entityType;
          var xml = serializer.serialize($scope[type], $scope.entityType);
          $scope.prettyXml = X2jsService.prettifyXml(xml);
          $scope.xml = xml;
          return xml;
        };

        $scope.saveEntity = function() {
          var type = $scope.entityType;
          var cleanedXml = cleanXml($scope.xml);
          SpinnersFlag.show = true;

          if($scope.editingMode) {
            Falcon.logRequest();

            Falcon.postUpdateEntity('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + cleanedXml, $scope.entityType, $scope[type].name)
              .success(function (response) {
                $scope.skipUndo = true;
                Falcon.logResponse('success', response, false);
                $state.go('main');
              })
              .error(function(err) {
                Falcon.logResponse('error', err, false);
                SpinnersFlag.show = false;
                angular.element('body, html').animate({scrollTop: 0}, 300);
              });
          } else {
            Falcon.logRequest();
            Falcon.postSubmitEntity('<?xml version="1.0" encoding="UTF-8" standalone="yes"?>' + cleanedXml, $scope.entityType)
              .success(function (response) {
                $scope.skipUndo = true;
                Falcon.logResponse('success', response, false);
                $state.go('main');
              })
              .error(function(err) {
                Falcon.logResponse('error', err, false);
                SpinnersFlag.show = false;
                angular.element('body, html').animate({scrollTop: 0}, 300);
              });
          }

          $scope.editingMode = false;
          $scope.cloningMode = false;
        };

        $scope.isActive = function (route) {
          return route === $state.current.name;
        };

        $scope.isCompleted = function (route) {
          return $state.get(route).data && $state.get(route).data.completed;
        };

        $scope.parseDate = function(input) {
          return input ? input.split('T')[0] : input;
        };

        $scope.parseTime = function(input) {
          if (input) {
            var partialTime = input.split('T')[1].split(':');
            partialTime = partialTime[0] + ':' + partialTime[1];
            return partialTime;
          }
          return 'Not defined';
        };

        $scope.appendVariable = function(timeVariable, holder, fieldName) {
          holder[fieldName] = holder[fieldName] ? (holder[fieldName] + '-' + timeVariable) : timeVariable;
          holder.focused = false;
        };

        var xmlPreviewCallback = function() {
          var type = $scope.entityType;
          if($scope.editXmlDisabled) {
            try {
              $scope.transform();
            } catch (exception) {
              console.log('error when transforming xml');
              console.log(exception);
            }
          } else {
            try {
              $scope[type] = serializer.deserialize($scope.prettyXml, type);
              $scope.invalidXml = false;
            } catch (exception) {
              $scope.invalidXml = true;
              console.log('user entered xml incorrect format');
              console.log(exception);
            }
          }

        };

        $scope.$watch('feed', xmlPreviewCallback, true);
        $scope.$watch('prettyXml', xmlPreviewCallback, true);
        $scope.$watch('feed.storage.catalog.catalogTable.uri',function(newValue, oldValue){
            $scope.feed.clusters.forEach(function(cluster){
              cluster.storage.catalog.catalogTable.uri = newValue;
            });
        });

        $scope.skipUndo = false;
        $scope.$on('$destroy', function () {

          var defaultFeed = entityFactory.newEntity('feed'),

              nameIsEqual = ($scope.feed.name == null || $scope.feed.name === ""),
              groupsIsEqual = ($scope.feed.groups == null || $scope.feed.groups === ""),
              descriptionIsEqual = ($scope.feed.description === null || $scope.feed.description === ""),
              ACLIsEqual = angular.equals($scope.feed.ACL, defaultFeed.ACL),
              schemaIsEqual = angular.equals($scope.feed.schema, defaultFeed.schema);

          if (!$scope.skipUndo && (!nameIsEqual || !groupsIsEqual || !descriptionIsEqual || !ACLIsEqual || !schemaIsEqual)) {
            $scope.$parent.models.feedModel = angular.copy(X2jsService.xml_str2json($scope.xml));
            if($scope.cloningMode){
              $scope.$parent.models.feedModel.clone = true;
            }
            if($scope.editingMode){
              $scope.$parent.models.feedModel.edit = true;
            }
            $scope.$parent.cancel('feed', $rootScope.previousState);
          }
        });
        $scope.goNext = function (formInvalid) {
          $state.current.data = $state.current.data || {};
          $state.current.data.completed = !formInvalid;

          SpinnersFlag.show = true;

          if (!validationService.nameAvailable || formInvalid) {
            validationService.displayValidations.show = true;
            validationService.displayValidations.nameShow = true;
            SpinnersFlag.show = false;
            if ($scope.currentState == 'forms.feed.clusters') {
              $scope.$broadcast('forms.feed.clusters:submit');
            }
            return;
          }
          if ($scope.currentState == 'forms.feed.clusters') {
            var sourceClustersCount = $scope.feed.clusters.filter(function(obj) {
              return obj.type == 'source'; }
            ).length;
            if(sourceClustersCount < 1) {
              SpinnersFlag.show = false;
              return;
            }
          }
          validationService.displayValidations.show = false;
          validationService.displayValidations.nameShow = false;
          $state.go(RouteHelper.getNextState($state.current.name, stateMatrix));
        };
        $scope.goBack = function () {
          SpinnersFlag.backShow = true;
          validationService.displayValidations.show = false;
          validationService.displayValidations.nameShow = false;
          $state.go(RouteHelper.getPreviousState($state.current.name, stateMatrix));
        };



        function cleanXml (xml) {

          var obj = X2jsService.xml_str2json(xml),
              clusterLocationsArray = [],
              feedLocationsArray = [];

          // cluster locations
          if(obj.feed.clusters.cluster[0].locations){
            obj.feed.clusters.cluster[0].locations.location.forEach(function (item) {
              if (item._path) {
                clusterLocationsArray.push(item);
              }
            });
          }

          if (clusterLocationsArray.length === 0) {
            delete obj.feed.clusters.cluster[0].locations;
          } else {
            obj.feed.clusters.cluster[0].locations.location = clusterLocationsArray;
          }

          // feed locations
          if(obj.feed.locations){
            obj.feed.locations.location.forEach(function (item) {
              if (item._path) {
                feedLocationsArray.push(item);
              }
            });
          }

          if (feedLocationsArray.length === 0) {
            delete obj.feed.locations;
          } else {
            obj.feed.locations.location = feedLocationsArray;
          }

          //feed properties
          if (obj.feed.properties.property.length === 1 && obj.feed.properties.property[0] === "") {
            delete obj.feed.properties;
          }

          return X2jsService.json2xml_str(obj);

        }

         $scope.toggleclick = function () {
              $('.formBoxContainer').toggleClass('col-xs-14 ');
              $('.xmlPreviewContainer ').toggleClass('col-xs-10 hide');
              $('.preview').toggleClass('pullOver pullOverXml');
              ($('.preview').hasClass('pullOver')) ? $('.preview').find('button').html('Preview XML') : $('.preview').find('button').html('Hide XML');
              ($($("textarea")[0]).attr("ng-model") == "prettyXml" ) ? $($("textarea")[0]).css("min-height", $(".formBoxContainer").height() - 40 ) : '';
          };

          if($state.current.name !== "forms.feed.general"){
            $state.go("forms.feed.general");
          }
      }]);


})();
