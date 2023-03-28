
CREATE TABLE IF NOT EXISTS fordav.vehicle
(
vehicleid VARCHAR(255) NOT NULL ENCODE lzo
,description VARCHAR(255) ENCODE lzo
,PRIMARY KEY (vehicleid)
)
DISTSTYLE ALL
;