# Problemas encontrados

- Precision Timestamps
- last_updated_at
- partitions
- packages/jars en spark (sobretodo en Kafka por el delta_builder)
- descarga de spark-4.0.1 de 30min hasta que arranca airflow (conexiones lentas) si se cambia algo del docker-compose.yml
- jars en airflow (jars-nimio)
- fechas datos de prueba SQL/producers csv
- Out of memory en Docker
- tener muchos streams abiertos
- delta pip builder no pillaba el `org.apache.spark:spark-token-provider-kafka-0-10_2.13:4.0.1`
- smartpool_config reutilizado en muchas libretas no me servia para kafka
- env variables (valores por defecto)
- workers spark