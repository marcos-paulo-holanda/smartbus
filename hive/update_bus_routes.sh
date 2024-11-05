#!/bin/bash
CURRENT_DATE=$(date +%Y-%m-%d)

# Atualiza a tabela de rotas
hive -e "ALTER TABLE busdata.bus_routes ADD PARTITION (data='${CURRENT_DATE}') LOCATION 's3a://trusted/busroutes/${CURRENT_DATE}';"