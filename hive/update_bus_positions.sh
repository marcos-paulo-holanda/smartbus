# comando necessário antes do agendamento
# apt-get update
# apt-get install -y cron
# apt-get install -y nano
#!/bin/bash
CURRENT_DATE=$(date +%Y-%m-%d)

# Solta a partição existente (se existir)
beeline -u "jdbc:hive2://localhost:10000" -e "ALTER TABLE busdata.bus_positions DROP PARTITION (data='${CURRENT_DATE}');"
beeline -u "jdbc:hive2://localhost:10000" -e "DROP VIEW IF EXISTS busdata.vw_bus_positions;"


# Atualiza a tabela de posições a cada 3 minutos
beeline -u "jdbc:hive2://localhost:10000" -e "ALTER TABLE busdata.bus_positions ADD PARTITION (data='${CURRENT_DATE}') LOCATION 's3a://trusted/buspositions/${CURRENT_DATE}';"
beeline -u "jdbc:hive2://localhost:10000" -e "CREATE VIEW IF NOT EXISTS busdata.vw_bus_positions AS SELECT * FROM busdata.bus_positions where data = '2024-11-01';"

# agendamento no cron
# */3 * * * * /opt/hive/update_bus_positions.sh