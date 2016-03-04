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

	var serverMessagesModule = angular.module('app.directives.server-messages', []);

	serverMessagesModule.directive('serverMessages', ["$rootScope", "$timeout", function ($rootScope, $timeout) {
		return {
			replace:false,
			restrict: 'E',
			templateUrl: 'html/directives/serverMessagesDv.html',
      link: function (scope, element) {

        //scope.allMessages
        var hideoutTimer;
        var notifyPanel = element.find(".notifs");
        $rootScope.$on('hideNotifications', function(setting) {
          $timeout.cancel(hideoutTimer);
          if (setting && setting.delay) {
            hideoutTimer = $timeout(function () {
              notifyPanel.fadeOut(300);
            }, setting.delay==='slow'?5000:0);
          } else {
            notifyPanel.stop();
            notifyPanel.fadeOut(300);
          }
        });

        $rootScope.$on('flashNotifications', function() {
          $timeout.cancel(hideoutTimer);
          notifyPanel.stop();
          notifyPanel.hide();
          notifyPanel.fadeIn(300);
          notifyPanel.fadeOut(300);
          notifyPanel.fadeIn(300);
          notifyPanel.fadeOut(300);
          notifyPanel.fadeIn(300);
        });

        $rootScope.$on('showNotifications', function() {
          $timeout.cancel(hideoutTimer);
          notifyPanel.stop();
          notifyPanel.hide();
          notifyPanel.fadeIn(300);
        });

      }
		};
	}]);

})();