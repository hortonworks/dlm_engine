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
--

create table beacon_job_details (
         sched_name varchar(120) not null,
         job_name varchar(200) not null,
         job_group varchar(200) not null,
         description varchar(250) ,
         job_class_name varchar(250) not null,
         is_durable varchar(5) not null,
         is_nonconcurrent varchar(5) not null,
         is_update_data varchar(5) not null,
         requests_recovery varchar(5) not null,
         job_data blob,
         primary key (sched_name,job_name,job_group)
         );

create table beacon_triggers (
         sched_name varchar(120) not null,
         trigger_name varchar(200) not null,
         trigger_group varchar(200) not null,
         job_name varchar(200) not null,
         job_group varchar(200) not null,
         description varchar(250),
         next_fire_time bigint,
         prev_fire_time bigint,
         priority integer,
         trigger_state varchar(16) not null,
         trigger_type varchar(8) not null,
         start_time bigint not null,
         end_time bigint,
         calendar_name varchar(200),
         misfire_instr smallint,
         job_data blob,
         primary key (sched_name,trigger_name,trigger_group),
         foreign key (sched_name,job_name,job_group) references beacon_job_details(sched_name,job_name,job_group)
         );

create table beacon_simple_triggers (
         sched_name varchar(120) not null,
         trigger_name varchar(200) not null,
         trigger_group varchar(200) not null,
         repeat_count bigint not null,
         repeat_interval bigint not null,
         times_triggered bigint not null,
         primary key (sched_name,trigger_name,trigger_group),
         foreign key (sched_name,trigger_name,trigger_group) references beacon_triggers(sched_name,trigger_name,trigger_group)
         );

create table beacon_cron_triggers (
         sched_name varchar(120) not null,
         trigger_name varchar(200) not null,
         trigger_group varchar(200) not null,
         cron_expression varchar(120) not null,
         time_zone_id varchar(80),
         primary key (sched_name,trigger_name,trigger_group),
         foreign key (sched_name,trigger_name,trigger_group) references beacon_triggers(sched_name,trigger_name,trigger_group)
         );

create table beacon_simprop_triggers (
         sched_name varchar(120) not null,
         trigger_name varchar(200) not null,
         trigger_group varchar(200) not null,
         str_prop_1 varchar(512),
         str_prop_2 varchar(512),
         str_prop_3 varchar(512),
         int_prop_1 int,
         int_prop_2 int,
         long_prop_1 bigint,
         long_prop_2 bigint,
         dec_prop_1 numeric(13,4),
         dec_prop_2 numeric(13,4),
         bool_prop_1 varchar(5),
         bool_prop_2 varchar(5),
         primary key (sched_name,trigger_name,trigger_group),
         foreign key (sched_name,trigger_name,trigger_group)
         references beacon_triggers(sched_name,trigger_name,trigger_group)
         );

create table beacon_blob_triggers (
         sched_name varchar(120) not null,
         trigger_name varchar(200) not null,
         trigger_group varchar(200) not null,
         blob_data blob,
         primary key (sched_name,trigger_name,trigger_group),
         foreign key (sched_name,trigger_name,trigger_group) references beacon_triggers(sched_name,trigger_name,trigger_group)
         );

create table beacon_calendars (
         sched_name varchar(120) not null,
         calendar_name varchar(200) not null,
         calendar blob not null,
         primary key (sched_name,calendar_name)
         );

create table beacon_paused_trigger_grps (
         sched_name varchar(120) not null,
         trigger_group varchar(200) not null,
         primary key (sched_name,trigger_group)
         );

create table beacon_fired_triggers (
         sched_name varchar(120) not null,
         entry_id varchar(95) not null,
         trigger_name varchar(200) not null,
         trigger_group varchar(200) not null,
         instance_name varchar(200) not null,
         fired_time bigint not null,
         sched_time bigint not null,
         priority integer not null,
         state varchar(16) not null,
         job_name varchar(200),
         job_group varchar(200),
         is_nonconcurrent varchar(5),
         requests_recovery varchar(5),
         primary key (sched_name,entry_id)
         );

create table beacon_scheduler_state (
         sched_name varchar(120) not null,
         instance_name varchar(200) not null,
         last_checkin_time bigint not null,
         checkin_interval bigint not null,
         primary key (sched_name,instance_name)
         );

create table beacon_locks (
         sched_name varchar(120) not null,
         lock_name varchar(40) not null,
         primary key (sched_name,lock_name)
         );

create table chained_jobs (
         id bigint generated by default as identity (start with 1, increment by 1),
         first_job_name varchar(40) not null,
         first_job_group varchar(40) not null,
         second_job_name varchar(40) not null,
         second_job_group varchar(40) not null,
        created_time bigint not null,
         primary key(id)
         );

create table policy_instance (
         id varchar(80) not null,
         class_name varchar(80) not null,
         name varchar(80) not null,
         job_execution_type varchar(80),
         job_type varchar(80) not null,
         start_time timestamp,
         end_time timestamp,
         frequency int,
         duration bigint,
         deleted int,
         status varchar(40),
         message varchar(255),
         primary key (id)
         );

CREATE TABLE policy (
        id bigint generated by default as identity (start with 1, increment by 1),
        name VARCHAR (100),
        version INTEGER,
        change_id INTEGER,
        status VARCHAR(20),
        type VARCHAR(20),
        source_cluster VARCHAR(255),
        target_cluster VARCHAR(255),
        source_dataset VARCHAR(4000),
        target_dataset VARCHAR(4000),
        created_time TIMESTAMP,
        modified_time TIMESTAMP,
        start_time TIMESTAMP,
        end_time TIMESTAMP,
        frequency INTEGER,
        notification_type VARCHAR(255),
        notification_to VARCHAR(255),
        retry_count INT,
        retry_delay INT,
        tags VARCHAR(1024),
        execution_type VARCHAR(40),
        deletion_time TIMESTAMP,
        PRIMARY KEY (id)
        );

CREATE TABLE policy_prop (
        id bigint generated by default as identity (start with 1, increment by 1),
        policy_id BIGINT,
        created_time TIMESTAMP,
        name VARCHAR(512),
        value VARCHAR(1024),
        type VARCHAR(20),
        PRIMARY KEY (id)
        );