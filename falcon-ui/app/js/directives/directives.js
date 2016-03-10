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

  var directivesModule = angular.module('app.directives', [
    'app.services',
    //'app.directives.entities-list',
    'app.directives.entities-search-list',
    'app.directives.instances-list',
    'app.directives.server-messages',
    'app.directives.entity',
    'app.directives.check-name',
    'app.directives.validation-message',
    'chart-module',
    'app.directives.dependencies-graph',
    'app.directives.lineage-graph',
    'tooltip',
    'app.directives.feed-cluster-partitions',
    'app.directives.acl-permissions',
    'app.directives.interface-endpoint'
  ]);

  directivesModule.directive('errorNav', function () {
    return {
      replace: false,
      restrict: 'A',
      templateUrl: 'html/error.html'
    };
  });

  directivesModule.directive('navHeader', function () {
    return {
      replace: false,
      restrict: 'A',
      templateUrl: 'html/directives/navDv.html',
      controller: 'HeaderController'
    };
  });

  //Angular is not supporting file inputs on change binding that is why this directive
  directivesModule.directive('fileinputChange', function () {
    return {
      restrict: "A",
      link: function (scope, element, attrs) {
        var onChangeFunc = element.scope()[attrs.fileinputChange];
        element.bind('change', onChangeFunc);
        element.bind('click', function () {
          this.value = '';
        });
      }
    };
  });

  directivesModule.factory('EncodeService', function () {
    return {
      encode: function (data) {
        return encodeURIComponent(data);
      }
    };
  });

  directivesModule.directive('frequency', function () {
    return {
      replace: false,
      scope: {
        value: "=",
        prefix: "@"
      },
      restrict: 'E',
      template: '{{output}}',
      link: function (scope) {
        if (scope.value.quantity) {
          scope.output = scope.prefix + ' ' + scope.value.quantity + ' ' + scope.value.unit;
        } else {
          scope.output = 'Not specified';
        }
      }
    };
  });

  directivesModule.directive('timeZoneSelect', function () {
    return {
      restrict: 'E',
      replace: false,
      scope: {
        ngModel: '=',
        required: '='
      },
      templateUrl: 'html/directives/timeZoneSelectDv.html'
    };
  });

  directivesModule.directive('simpleDate', ['$filter', function ($filter) {
    return {
      require: 'ngModel',
      link: function (scope, element, attrs, ngModelController) {
        ngModelController.$parsers.push(function (data) {
          //convert data from view format to model format
          return data;
        });
        ngModelController.$formatters.push(function (date) {
          //convert data from model format to view format
          if (date !== "") {
            date = $filter('date')(date, 'MM/dd/yyyy');
          }
          return date;
        });
      }
    };
  }]);

  directivesModule.directive('ngEnter', function () {
    return function (scope, element, attrs) {
      element.bind("keydown keypress", function (event) {
        if (event.which === 13) {
          scope.$apply(function () {
            scope.$eval(attrs.ngEnter);
          });
          event.preventDefault();
        }
      });
    };
  });

  directivesModule.directive('elastic', ['$timeout', function ($timeout) {
    return {
      restrict: 'A',
      link: function ($scope, element) {
        $scope.$watch(function () {
          return element[0].value;
        }, function () {
          resize();
        });
        var resize = function () {
          element[0].style.resize = "vertical";
          element[0].style.height = "250px";
          return element[0].style.height = "" + element[0].scrollHeight + "px";
        };
        $timeout(resize, 0);
      }
    };
  }
  ]);

  directivesModule.directive('autofocus', ['$timeout', function ($timeout) {
    return {
      restrict: 'A',
      link: function ($scope, element) {
        $timeout(function () {
          element.trigger('focus');
        }, 20);
      }
    };
  }
  ]);

  directivesModule.filter('dateFormatter', function () {
    return function (date) {
      console.log(date);
      var dates = date.split('T')[0],
        time = date.split('T')[1].split('Z')[0].split('.')[0];
      return dates + ' ' + time;
    };
  });

  directivesModule.directive('onBlur', [function () {
    return {
      restrict: 'A',
      link: function (scope, elm, attrs) {
        elm.bind('blur', function () {
          if (attrs.onBlur)
            scope[attrs.onBlur]();
          else
            return false;
        });
      }
    };
  }]);

  directivesModule.directive('scrollToError', ['$timeout',function ($timeout) {
      return {
          require : "^form",
          restrict : 'A',
          link: function (scope, element,attrs,form) {
              element.on('mousedown',function(event){
                event.preventDefault();
              });
              element.on('click', function () {
                  var formElement = angular.element('form[name="' + form.$name + '"]');
                  var firstInvalid = formElement[0].querySelector('.ng-invalid');
                  $timeout(function() {
                    if (firstInvalid) {
                      firstInvalid.blur();
                      firstInvalid.focus();
                    }
                  },0)
              });
          }
      };
  }]);

  directivesModule.directive('feedFormClusterDetails', function () {
    return {
      replace: false,
      restrict: 'EA',
      templateUrl: 'html/feed/feedFormClusterDetailsTpl.html',
      link: function ($scope, $element) {
        $scope.$on('forms.feed.clusters:submit', function() {
          $scope.cluster.isAccordionOpened = $element.find('.ng-invalid').length > 0;
        });
      }
    };
  });

  directivesModule.directive('mandatoryField', function () {
    return {
      replace: false,
      restrict: 'E',
      template: '<span>*</span>'
    };
  });

}());
