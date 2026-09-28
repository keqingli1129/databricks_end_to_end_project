"""Connection settings for Confluent Cloud Kafka (SASL_SSL + PLAIN, API key/secret)."""

SECRET_KEYS = ("bootstrap_servers", "api_key", "api_secret")


def read_credentials(dbutils, scope: str) -> tuple[str, str, str]:
    """Read (bootstrap_servers, api_key, api_secret) from a Databricks secret scope."""
    servers, key, secret = (dbutils.secrets.get(scope, name) for name in SECRET_KEYS)
    return servers, key, secret


def spark_kafka_options(bootstrap_servers: str, api_key: str, api_secret: str, topic: str) -> dict[str, str]:
    """Options for spark.readStream.format("kafka") on Databricks."""
    # Databricks ships a shaded Kafka client, hence the "kafkashaded." class prefix.
    jaas = (
        "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
        f'username="{api_key}" password="{api_secret}";'
    )
    return {
        "kafka.bootstrap.servers": bootstrap_servers,
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": jaas,
        "subscribe": topic,
        "startingOffsets": "earliest",
    }


def producer_config(bootstrap_servers: str, api_key: str, api_secret: str) -> dict[str, str]:
    """Config for confluent_kafka.Producer."""
    return {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": api_key,
        "sasl.password": api_secret,
    }
