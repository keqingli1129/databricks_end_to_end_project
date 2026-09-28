from types import SimpleNamespace

from databricks_end_to_end_project.telematics import kafka_config


def test_spark_kafka_options_for_confluent():
    opts = kafka_config.spark_kafka_options("<BOOTSTRAP_SERVER>", "<API_KEY>", "<API_SECRET>", "telematics")

    assert opts == {
        "kafka.bootstrap.servers": "<BOOTSTRAP_SERVER>",
        "kafka.security.protocol": "SASL_SSL",
        "kafka.sasl.mechanism": "PLAIN",
        "kafka.sasl.jaas.config": (
            "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required "
            'username="<API_KEY>" password="<API_SECRET>";'
        ),
        "subscribe": "telematics",
        "startingOffsets": "earliest",
    }


def test_producer_config_for_confluent():
    conf = kafka_config.producer_config("<BOOTSTRAP_SERVER>", "<API_KEY>", "<API_SECRET>")

    assert conf == {
        "bootstrap.servers": "<BOOTSTRAP_SERVER>",
        "security.protocol": "SASL_SSL",
        "sasl.mechanisms": "PLAIN",
        "sasl.username": "<API_KEY>",
        "sasl.password": "<API_SECRET>",
    }


def test_read_credentials_reads_the_three_keys_from_the_scope():
    calls = []

    def get(scope, key):
        calls.append((scope, key))
        return f"{key}-value"

    fake_dbutils = SimpleNamespace(secrets=SimpleNamespace(get=get))

    assert kafka_config.read_credentials(fake_dbutils, "kafka_dev") == (
        "bootstrap_servers-value",
        "api_key-value",
        "api_secret-value",
    )
    assert calls == [("kafka_dev", "bootstrap_servers"), ("kafka_dev", "api_key"), ("kafka_dev", "api_secret")]
