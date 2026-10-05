import json
import os
import re
import yaml

from delta.tables import DeltaTable
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    concat_ws,
    current_timestamp,
    from_json,
    get_json_object,
    lit,
    sha2,
    struct,
    to_date,
    to_json,
    udf,
)
from pyspark.sql.types import BooleanType, FloatType, StringType, StructField, StructType

LAKEHOUSE_ROOT = os.getenv("LAKEHOUSE_ROOT", "/lakehouse")
BRONZE_PATH = f"{LAKEHOUSE_ROOT}/bronze/events"
SILVER_PATH = f"{LAKEHOUSE_ROOT}/silver/validated_objects"
QUARANTINE_PATH = f"{LAKEHOUSE_ROOT}/silver/quarantine"
GOLD_ROOT = f"{LAKEHOUSE_ROOT}/gold"
CHECKPOINT_PATH = os.getenv("CHECKPOINT_PATH", "/checkpoints/ontoflow-v3")

spark = (
    SparkSession.builder.appName("OntoFlowDeltaOntologyStreaming")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    .config("spark.es.nodes", "elasticsearch")
    .config("spark.es.port", "9200")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

with open("/ontology/ontology.yaml") as f:
    ontology = yaml.safe_load(f)
objects = ontology["objects"]

# wtf iknow
all_props = {}
for obj in objects.values():
    for prop, spec in obj["properties"].items():
        all_props[prop] = spec

fields = [StructField("object_type", StringType())]
for prop, spec in sorted(all_props.items()):
    typ = spec.get("type", "string")
    spark_type = FloatType() if typ == "float" else BooleanType() if typ == "boolean" else StringType()
    fields.append(StructField(prop, spark_type))
fields += [StructField("source_version", StringType()), StructField("payload", StringType())]
schema = StructType(fields)

raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", "kafka:29092")
    .option("subscribe", "raw.events,raw.documents")
    .option("startingOffsets", "earliest")
    .load()
    .select(
        col("topic"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        col("key").cast("string").alias("kafka_key"),
        col("value").cast("string").alias("_raw_json"),
    )
)

parsed = raw.select(
    "topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "kafka_key",
    "_raw_json",
    from_json(col("_raw_json"), schema).alias("d"),
).select(
    "topic",
    "kafka_partition",
    "kafka_offset",
    "kafka_timestamp",
    "kafka_key",
    "_raw_json",
    "d.*",
)

legacy = {
    "production_event": "Batch",
    "quality_event": "QualityFinding",
    "maintenance_action": "MaintenanceAction",
    "document": "EngineeringDocument",
}


@udf(StringType())
def normalize_type(value):
    return legacy.get(value, value)


@udf(StringType())
def validate_json(object_type, payload_json):
    if object_type not in objects:
        return json.dumps(
            {"valid": False, "errors": [f"unknown object_type {object_type}"], "score": 0.0}
        )

    data = json.loads(payload_json or "{}")
    spec = objects[object_type]
    errors = []
    checked = 0
    passed = 0

    for prop, rule in spec["properties"].items():
        value = data.get(prop)
        required = rule.get("required", False)
        checked += 1

        if required and (value is None or str(value).strip() == ""):
            errors.append(f"{prop}: required")
            continue
        if value is None:
            passed += 1
            continue
        if rule.get("type") == "enum" and value not in rule.get("values", []):
            errors.append(f"{prop}: invalid enum value {value}")
        else:
            passed += 1

    score = passed / max(checked, 1)
    threshold = ontology.get("constraints", {}).get("default_dq_min", 0.70)
    return json.dumps(
        {"valid": len(errors) == 0 and score >= threshold, "errors": errors, "score": round(score, 4)}
    )


normalized = (
    parsed.withColumn("object_type", normalize_type(col("object_type")))
    .withColumn("ingested_at", current_timestamp())
    .withColumn("ingest_date", to_date(col("kafka_timestamp")))
)
#rewrite this for document chunk connector
payload_cols = [
    col(c)
    for c in normalized.columns
    if c
    not in (
        "source_version",
        "payload",
        "topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_key",
        "_raw_json",
        "ingested_at",
        "ingest_date",
    )
]
normalized = normalized.withColumn("_payload_json", to_json(struct(*payload_cols)))
normalized = normalized.withColumn("_validation", validate_json(col("object_type"), col("_payload_json")))
normalized = (
    normalized.withColumn("valid", get_json_object(col("_validation"), "$.valid").cast("boolean"))
    .withColumn("dq_score", get_json_object(col("_validation"), "$.score").cast("float"))
    .withColumn("validation_errors", get_json_object(col("_validation"), "$.errors"))
    .withColumn("source_hash", sha2(concat_ws("||", col("object_type"), col("_raw_json")), 256))
)


def _delta_append_idempotent(df, path, app_id, epoch, partition_by=None):
    """Append a foreachBatch output to Delta exactly once per (app_id, epoch)."""
    writer = (
        df.write.format("delta")
        .mode("append")
        .option("txnAppId", app_id)
        .option("txnVersion", int(epoch))
    )
    if partition_by:
        writer = writer.partitionBy(*partition_by)
    writer.save(path)


def _safe_table_name(object_type):
    return re.sub(r"(?<!^)(?=[A-Z])", "_", object_type).lower()


def _upsert_gold(subset, object_type, spec):
    """Gold tables are current-state ontology tables keyed by each object's primary key."""
    pk = spec["primary_key"]
    table_path = f"{GOLD_ROOT}/{_safe_table_name(object_type)}"
    gold_cols = ["object_type"] + list(spec["properties"].keys()) + [
        "ingested_at",
        "source_hash",
        "dq_score",
        "valid",
    ]
    existing = [c for c in gold_cols if c in subset.columns]
    gold = subset.select(*existing).filter(col(pk).isNotNull()).dropDuplicates([pk])

    if not DeltaTable.isDeltaTable(spark, table_path):
        gold.write.format("delta").mode("overwrite").save(table_path)
    else:
        target = DeltaTable.forPath(spark, table_path)
        (
            target.alias("t")
            .merge(gold.alias("s"), f"t.`{pk}` = s.`{pk}`")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )

    return gold


def write_batch(df, epoch):
    #bronze replayable raw truth.
    bronze = df.select(
        "topic",
        "kafka_partition",
        "kafka_offset",
        "kafka_timestamp",
        "kafka_key",
        "_raw_json",
        "ingested_at",
        "ingest_date",
        "source_hash",
    )
    _delta_append_idempotent(
        bronze,
        BRONZE_PATH,
        "ontoflow-bronze",
        epoch,
        partition_by=["ingest_date"],
    )

    valid = df.filter(col("valid") == True)
    invalid = df.filter(col("valid") != True)

    # SILVER: normalized + ontology validated records with DQ and provenance.
    silver_cols = [
        c
        for c in df.columns
        if c not in ("_validation",)
    ]
    _delta_append_idempotent(
        valid.select(*silver_cols),
        SILVER_PATH,
        "ontoflow-silver",
        epoch,
        partition_by=["object_type"],
    )

    # Invalid records are durable in Delta as well as exposed in Elasticsearch for operations.
    quarantine = invalid.select(
        "object_type",
        "ingested_at",
        "ingest_date",
        "dq_score",
        "validation_errors",
        "source_hash",
        "_raw_json",
        "topic",
        "kafka_partition",
        "kafka_offset",
    )
    _delta_append_idempotent(
        quarantine,
        QUARANTINE_PATH,
        "ontoflow-quarantine",
        epoch,
        partition_by=["ingest_date"],
    )

    if not invalid.rdd.isEmpty():
        (
            quarantine.drop("ingest_date")
            .write.format("org.elasticsearch.spark.sql")
            .option("es.resource", "quarantine")
            .mode("append")
            .save()
        )

    # GOLD: one typed, current-state Delta table per ontology object.
    # The exact same canonical rows are projected into Elasticsearch as the serving/search index.
    for object_type, spec in objects.items():
        subset = valid.filter(col("object_type") == object_type)
        if subset.rdd.isEmpty():
            continue
        gold = _upsert_gold(subset, object_type, spec)
        pk = spec["primary_key"]
        (
            gold.write.format("org.elasticsearch.spark.sql")
            .option("es.resource", spec["index"])
            .option("es.mapping.id", pk)
            .mode("append")
            .save()
        )


query = (
    normalized.writeStream.foreachBatch(write_batch)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .start()
)
query.awaitTermination()
