CREATE DATABASE IF NOT EXISTS busdata;

CREATE EXTERNAL TABLE IF NOT EXISTS busdata.passageiros (
	mes STRING,
    Linha STRING,
    Tot_Passageiros_Transportados STRING,
    Media_Diaria STRING,
    letreiro STRING,
    itinerario STRING
)
ROW FORMAT SERDE 'org.apache.hive.hcatalog.data.JsonSerDe'
STORED AS TEXTFILE
LOCATION 's3a://trusted/passageiros/2023';

CREATE VIEW IF NOT EXISTS busdata.vw_passageiros AS SELECT * FROM busdata.passageiros;

select * from busdata.passageiros where mes = 'Fevereiro' limit 15;


