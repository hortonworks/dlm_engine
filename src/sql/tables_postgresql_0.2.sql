-- Copyright  (c) 2016-2017, Hortonworks Inc.  All rights reserved.
--
-- Except as expressly permitted in a written agreement between you or your
-- company and Hortonworks, Inc. or an authorized affiliate or partner
-- thereof, any use, reproduction, modification, redistribution, sharing,
-- lending or other exploitation of all or any part of the contents of this
-- software is strictly prohibited.

CREATE TABLE IF NOT EXISTS BEACON_CLOUD_CRED (
  ID                  VARCHAR(128),
  NAME                VARCHAR(128),
  PROVIDER            VARCHAR(64),
  CREATION_TIME       TIMESTAMP NULL DEFAULT NULL,
  LAST_MODIFIED_TIME  TIMESTAMP NULL DEFAULT NULL,
  RETIREMENT_TIME     TIMESTAMP NULL DEFAULT NULL,
  CONFIGURATION       BYTEA,
  PRIMARY KEY (ID)
);

CREATE INDEX IF NOT EXISTS IDX_BEACON_CC_PR ON BEACON_CLOUD_CRED(PROVIDER);
CREATE INDEX IF NOT EXISTS IDX_BEACON_CC_NA ON BEACON_CLOUD_CRED(NAME);
