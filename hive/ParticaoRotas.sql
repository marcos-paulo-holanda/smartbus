CREATE EXTERNAL TABLE IF NOT EXISTS busdata.bus_routes (
    codigo_trajeto STRING,
    sentido STRING,
    letreiro STRING,
    terminal_primario STRING,
    terminal_secundario STRING,
    qnt_veiculos STRING
)
PARTITIONED BY (data STRING)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3a://trusted/busroutes';

ALTER TABLE busdata.bus_routes ADD PARTITION (data='2024-11-01') LOCATION 's3a://trusted/busroutes/2024-11-01';

CREATE VIEW IF NOT EXISTS busdata.vw_bus_routes AS SELECT * FROM busdata.bus_routes where data = '2024-11-01';

select count(*) from busdata.bus_routes where data = '2024-11-01';

select * from busdata.vw_bus_routes where data = '2024-11-01' limit 10;

