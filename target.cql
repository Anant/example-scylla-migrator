CREATE KEYSPACE demo WITH REPLICATION={'class': 'SimpleStrategy', 'replication_factor': 1};

use demo ;

CREATE TABLE IF NOT EXISTS spacecraft_journey_catalog (
  	spacecraft_name text,
  	journey_id timeuuid,
  	start timestamp,
  	end timestamp,
  	active boolean,
  	summary text,
  	PRIMARY KEY ((spacecraft_name), journey_id)
) WITH CLUSTERING ORDER BY (journey_id desc);