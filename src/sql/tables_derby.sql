-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.


-- Auto drop and reset tables
-- Derby doesn't support if exists condition on table drop, so user must manually do this step if needed to.
-- noinspection SqlDialectInspection

-- drop table beacon_blob_triggers;
-- drop table beacon_calendars;
-- drop table beacon_cron_triggers;
-- drop table beacon_fired_triggers;
-- drop table beacon_locks;
-- drop table beacon_scheduler_state;
-- drop table beacon_simprop_triggers;
-- drop table beacon_simple_triggers;
-- drop table beacon_triggers;
-- drop table beacon_paused_trigger_grps;
-- drop table beacon_job_details;
-- drop table chained_jobs;
-- drop table job_instance;
-- drop table policy;

CREATE TABLE beacon_job_details (
  sched_name        VARCHAR(120) NOT NULL,
  job_name          VARCHAR(200) NOT NULL,
  job_group         VARCHAR(200) NOT NULL,
  description       VARCHAR(250),
  job_class_name    VARCHAR(250) NOT NULL,
  is_durable        VARCHAR(5)   NOT NULL,
  is_nonconcurrent  VARCHAR(5)   NOT NULL,
  is_update_data    VARCHAR(5)   NOT NULL,
  requests_recovery VARCHAR(5)   NOT NULL,
  job_data          BLOB,
  PRIMARY KEY (sched_name, job_name, job_group)
);

CREATE TABLE beacon_triggers (
  sched_name     VARCHAR(120) NOT NULL,
  trigger_name   VARCHAR(200) NOT NULL,
  trigger_group  VARCHAR(200) NOT NULL,
  job_name       VARCHAR(200) NOT NULL,
  job_group      VARCHAR(200) NOT NULL,
  description    VARCHAR(250),
  next_fire_time BIGINT,
  prev_fire_time BIGINT,
  priority       INTEGER,
  trigger_state  VARCHAR(16)  NOT NULL,
  trigger_type   VARCHAR(8)   NOT NULL,
  start_time     BIGINT       NOT NULL,
  end_time       BIGINT,
  calendar_name  VARCHAR(200),
  misfire_instr  SMALLINT,
  job_data       BLOB,
  PRIMARY KEY (sched_name, trigger_name, trigger_group),
  FOREIGN KEY (sched_name, job_name, job_group) REFERENCES beacon_job_details (sched_name, job_name, job_group)
);

CREATE TABLE beacon_simple_triggers (
  sched_name      VARCHAR(120) NOT NULL,
  trigger_name    VARCHAR(200) NOT NULL,
  trigger_group   VARCHAR(200) NOT NULL,
  repeat_count    BIGINT       NOT NULL,
  repeat_interval BIGINT       NOT NULL,
  times_triggered BIGINT       NOT NULL,
  PRIMARY KEY (sched_name, trigger_name, trigger_group),
  FOREIGN KEY (sched_name, trigger_name, trigger_group) REFERENCES beacon_triggers (sched_name, trigger_name, trigger_group)
);

CREATE TABLE beacon_cron_triggers (
  sched_name      VARCHAR(120) NOT NULL,
  trigger_name    VARCHAR(200) NOT NULL,
  trigger_group   VARCHAR(200) NOT NULL,
  cron_expression VARCHAR(120) NOT NULL,
  time_zone_id    VARCHAR(80),
  PRIMARY KEY (sched_name, trigger_name, trigger_group),
  FOREIGN KEY (sched_name, trigger_name, trigger_group) REFERENCES beacon_triggers (sched_name, trigger_name, trigger_group)
);

CREATE TABLE beacon_simprop_triggers
(
  sched_name    VARCHAR(120) NOT NULL,
  trigger_name  VARCHAR(200) NOT NULL,
  trigger_group VARCHAR(200) NOT NULL,
  str_prop_1    VARCHAR(512),
  str_prop_2    VARCHAR(512),
  str_prop_3    VARCHAR(512),
  int_prop_1    INT,
  int_prop_2    INT,
  long_prop_1   BIGINT,
  long_prop_2   BIGINT,
  dec_prop_1    NUMERIC(13, 4),
  dec_prop_2    NUMERIC(13, 4),
  bool_prop_1   VARCHAR(5),
  bool_prop_2   VARCHAR(5),
  PRIMARY KEY (sched_name, trigger_name, trigger_group),
  FOREIGN KEY (sched_name, trigger_name, trigger_group)
  REFERENCES beacon_triggers (sched_name, trigger_name, trigger_group)
);

CREATE TABLE beacon_blob_triggers (
  sched_name    VARCHAR(120) NOT NULL,
  trigger_name  VARCHAR(200) NOT NULL,
  trigger_group VARCHAR(200) NOT NULL,
  blob_data     BLOB,
  PRIMARY KEY (sched_name, trigger_name, trigger_group),
  FOREIGN KEY (sched_name, trigger_name, trigger_group) REFERENCES beacon_triggers (sched_name, trigger_name, trigger_group)
);

CREATE TABLE beacon_calendars (
  sched_name    VARCHAR(120) NOT NULL,
  calendar_name VARCHAR(200) NOT NULL,
  calendar      BLOB         NOT NULL,
  PRIMARY KEY (sched_name, calendar_name)
);

CREATE TABLE beacon_paused_trigger_grps
(
  sched_name    VARCHAR(120) NOT NULL,
  trigger_group VARCHAR(200) NOT NULL,
  PRIMARY KEY (sched_name, trigger_group)
);

CREATE TABLE beacon_fired_triggers (
  sched_name        VARCHAR(120) NOT NULL,
  entry_id          VARCHAR(95)  NOT NULL,
  trigger_name      VARCHAR(200) NOT NULL,
  trigger_group     VARCHAR(200) NOT NULL,
  instance_name     VARCHAR(200) NOT NULL,
  fired_time        BIGINT       NOT NULL,
  sched_time        BIGINT       NOT NULL,
  priority          INTEGER      NOT NULL,
  state             VARCHAR(16)  NOT NULL,
  job_name          VARCHAR(200),
  job_group         VARCHAR(200),
  is_nonconcurrent  VARCHAR(5),
  requests_recovery VARCHAR(5),
  PRIMARY KEY (sched_name, entry_id)
);

CREATE TABLE beacon_scheduler_state
(
  sched_name        VARCHAR(120) NOT NULL,
  instance_name     VARCHAR(200) NOT NULL,
  last_checkin_time BIGINT       NOT NULL,
  checkin_interval  BIGINT       NOT NULL,
  PRIMARY KEY (sched_name, instance_name)
);

CREATE TABLE beacon_locks
(
  sched_name VARCHAR(120) NOT NULL,
  lock_name  VARCHAR(40)  NOT NULL,
  PRIMARY KEY (sched_name, lock_name)
);

CREATE TABLE chained_jobs
(
  id               BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ( START WITH 1, INCREMENT BY 1),
  created_time     BIGINT,
  first_job_group  VARCHAR(255),
  first_job_name   VARCHAR(255),
  second_job_group VARCHAR(255),
  second_job_name  VARCHAR(255),
  PRIMARY KEY (id)
);

CREATE TABLE policy_instance
(
  id                 VARCHAR(255) NOT NULL,
  class_name         VARCHAR(255),
  deleted            INTEGER,
  duration           BIGINT,
  end_time           TIMESTAMP,
  frequency          INTEGER,
  job_group          VARCHAR(255),
  job_name           VARCHAR(255),
  message            VARCHAR(4000),
  name               VARCHAR(255),
  start_time         TIMESTAMP,
  status             VARCHAR(255),
  job_type           VARCHAR(255),
  job_execution_type VARCHAR(80),
  PRIMARY KEY (id)
);

CREATE TABLE policy
(
  id                BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ( START WITH 1, INCREMENT BY 1),
  name              VARCHAR(100),
  version           INTEGER,
  change_id         INTEGER,
  status            VARCHAR(20),
  type              VARCHAR(20),
  source_cluster    VARCHAR(255),
  target_cluster    VARCHAR(255),
  source_dataset    VARCHAR(4000),
  target_dataset    VARCHAR(4000),
  created_time      TIMESTAMP,
  modified_time     TIMESTAMP,
  start_time        TIMESTAMP,
  end_time          TIMESTAMP,
  frequency         INTEGER,
  notification_type VARCHAR(255),
  notification_to   VARCHAR(255),
  retry_count       INT,
  retry_delay       INT,
  tags              VARCHAR(1024),
  execution_type    VARCHAR(40),
  deletion_time     TIMESTAMP,
  PRIMARY KEY (id)
);

CREATE TABLE policy_prop
(
  id           BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY ( START WITH 1, INCREMENT BY 1),
  policy_id    BIGINT,
  created_time TIMESTAMP,
  name         VARCHAR(512),
  value        VARCHAR(1024),
  type         VARCHAR(20),
  PRIMARY KEY (id)
);
