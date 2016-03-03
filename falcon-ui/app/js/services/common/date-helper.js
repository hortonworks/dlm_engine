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

  angular.module('dateHelper', [])
    .factory('DateHelper', function () {

      var formatDigit = function(digit){
        if(digit<10){
          digit = "0"+digit;
        }
        return digit;
      };

      var getTZOffset = function(tz){
        var zoneDeltaPortion = tz.slice(4);
        var zoneDeltaDirection = parseInt(tz.substr(3,1)+1);
        var timePortions = zoneDeltaPortion.split(":");
        var zoneTimeOffset = zoneDeltaDirection*parseInt(timePortions[0])+parseInt(timePortions[1])/60;
        return zoneTimeOffset;
      }

      var dateHelper = {};

      dateHelper.importDate = function (date, tz) {
        if (!tz || tz === 'UTC') {
          tz = "GMT+00:00";
        }
        var rawDate = Date.parse(date);
        var tzDate = new Date (rawDate + (3600000*getTZOffset(tz)));
        return new Date(
          tzDate.getUTCFullYear(),
          tzDate.getUTCMonth(),
          tzDate.getUTCDate(),
          tzDate.getUTCHours(),
          tzDate.getUTCMinutes(),
          0, 0);

      };

      dateHelper.createISO = function (date, time, tz) {
        var inputDate = new Date(date.getFullYear(),date.getMonth(),date.getDate(),
                time.getHours(),time.getMinutes());
        if (!tz || tz === 'UTC') {
            tz = "GMT+00:00";
        }
        var currentOffsetInHours= -1*inputDate.getTimezoneOffset()/60;
        var effectiveOff = currentOffsetInHours - getTZOffset(tz);
        var inputDateInCurrentTz = inputDate.getTime() + effectiveOff*60*60*1000;
        var inputDateInUTC = new Date(inputDateInCurrentTz - currentOffsetInHours*60*60*1000);
        return dateHelper.createISOString(inputDateInUTC,inputDateInUTC);
      };

      //i.e. 2015-09-10T16:35:21.235Z
      dateHelper.createISOString = function (date, time) {
        var result = date.getFullYear() + "-" + formatDigit(date.getMonth()+1) + "-" + formatDigit(date.getDate())
          + "T" + formatDigit(time.getHours()) + ":" + formatDigit(time.getMinutes()) + "Z";
        return result;
      };

      dateHelper.getDateTimeString = function(date,time){
        var inputDate = new Date(date.getFullYear(),date.getMonth(),date.getDate(),
                time.getHours(),time.getMinutes());
        var result = inputDate.getFullYear() + "-" + formatDigit(inputDate.getMonth()+1) + "-" + formatDigit(inputDate.getDate())
          + " " + formatDigit(inputDate.getHours()>12?inputDate.getHours()-12:inputDate.getHours()) + ":" + formatDigit(inputDate.getMinutes());
        if(inputDate.getHours() < 12){
          result = result + " AM";
        }else{
          result = result + " PM";
        }
        return result;
      }
      return dateHelper;

    });

})();
