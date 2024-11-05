FROM fjardim/mds-hive-metastore

# Copiar os scripts para o container
COPY hive/update_bus_positions.sh /opt/hive/update_bus_positions.sh
COPY hive/update_bus_routes.sh /opt/hive/update_bus_routes.sh

# Garantir permissões de execução para os scripts
RUN chmod +x /opt/hive/update_bus_positions.sh /opt/hive/update_bus_routes.sh && \
    apt-get update && apt-get install -y cron && \
    (echo "*/3 * * * * /opt/hive/update_bus_positions.sh" >> /etc/crontab) && \
    (echo "0 0 * * * /opt/hive/update_bus_routes.sh" >> /etc/crontab)

# Iniciar o serviço cron
CMD service cron start && tail -f /dev/null
