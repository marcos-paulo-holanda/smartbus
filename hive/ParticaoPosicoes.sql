CREATE EXTERNAL TABLE IF NOT EXISTS busdata.bus_positions (
    codigo_trajeto STRING,
    sentido STRING,
    prefixo_veiculo STRING,
    latitude DOUBLE,
    longitude DOUBLE,
    horario_posicao STRING
)
PARTITIONED BY (data STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3a://trusted/buspositions';


ALTER TABLE busdata.bus_positions DROP PARTITION (data='2024-11-01');

ALTER TABLE busdata.bus_positions ADD PARTITION (data='2024-11-01') LOCATION 's3a://trusted/buspositions/2024-11-01';


DROP VIEW IF EXISTS busdata.vw_bus_positions;

CREATE VIEW IF NOT EXISTS busdata.vw_bus_positions AS SELECT * FROM busdata.bus_positions where data = '2024-11-01';

select count(*) from busdata.bus_positions where data = '2024-11-01';

select count(*) from busdata.vw_bus_positions;