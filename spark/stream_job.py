import findspark
# Automatically link your local virtual environment libraries
findspark.init(r"E:\pro_p\fraud\env\Lib\site-packages\pyspark")

from pyspark.sql import SparkSession, functions as F, types as T
import joblib
import numpy as np
import os


os.environ["HADOOP_HOME"] = r"C:\hadoop"
os.environ["hadoop.home.dir"] = r"C:\hadoop"

MODEL_PATH = r"E:\pro_p\fraud\ml\model.pkl"
PG_URL  = "jdbc:postgresql://localhost:5432/fraud"
PG_PROP = {"user": "fraud", "password": "fraud", "driver": "org.postgresql.Driver"}

# 1. Define explicit schemas matching incoming payload structures
schema = T.StructType([
    T.StructField("tx_id", T.StringType()), T.StructField("user_id", T.StringType()),
    T.StructField("amount", T.DoubleType()), T.StructField("merchant", T.StringType()),
    T.StructField("country", T.StringType()), T.StructField("device", T.StringType()),
    T.StructField("ts", T.StringType()),
])

# 2. Build session using standard compatible versions (Match your packages exactly)
spark = (
    SparkSession.builder
    .appName("fraud-stream")
    
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.hadoop.hadoop.home.dir", r"C:\hadoop")
    .config("spark.hadoop.fs.file.impl", "org.apache.hadoop.fs.LocalFileSystem")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# 3. Load and broadcast the pipeline bundle safely
bundle = joblib.load(MODEL_PATH)
bc_bundle = spark.sparkContext.broadcast(bundle)

# 4. Corrected User Defined Function (UDF)
@F.udf(returnType=T.StructType([
    T.StructField("risk_score", T.FloatType()),
    T.StructField("is_fraud",   T.BooleanType()),
    T.StructField("reason",     T.StringType()),
]))
def score(amount, country, device, merchant):
    # Resolve values out of the broadcast frame context safely
    local_bundle = bc_bundle.value
    encoder = local_bundle["encoder"]
    clf_model = local_bundle["model"]
    
    # Fallback mappings for missing values
    c = country if country is not None else "NA"
    d = device if device is not None else "NA"
    m = merchant if merchant is not None else "NA"
    amt = float(amount) if amount is not None else 0.0
    
    # Process categorical encodings via pipeline array states
    encoded_cats = encoder.transform([[c, d, m]])
    if hasattr(encoded_cats, "toarray"):
        encoded_cats = encoded_cats.toarray()
        
    # Combine structural features explicitly
    feats = np.hstack([[[amt]], encoded_cats])
    
    # Run Inference
    prob = float(clf_model.predict_proba(feats)[0, 1])
    is_anomaly = bool(prob > 0.7)
    msg = "high amount + rare geo" if is_anomaly else ""
    
    return (prob, is_anomaly, msg)

# 5. Read Kafka Streams
raw = (spark.readStream.format("kafka")
       .option("kafka.bootstrap.servers", "localhost:9092")
       .option("subscribe", "transactions")
       .option("startingOffsets", "latest")
       .load())

# 6. Structuring Streaming Context Transformation Layers
df = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("d")).select("d.*")
        .withColumn("s", score("amount", "country", "device", "merchant"))
        .select("tx_id", "user_id", "amount", "merchant", "country", "device",
                F.to_timestamp("ts").alias("ts"),
                F.col("s.risk_score").alias("risk_score"),
                F.col("s.is_fraud").alias("is_fraud"),
                F.col("s.reason").alias("reason")))

# 7. High Performance Database Sink (Bypassing expensive explicit counts)
def write_batch(batch_df, epoch_id):
    if batch_df.isStreaming or batch_df.storageLevel.useMemory:
        batch_df.cache()
        
    # Append baseline transaction history records
    (batch_df.drop("reason")
     .write.jdbc(PG_URL, "transactions", "append", PG_PROP))
     
    # Append triggered alerts records seamlessly
    alerts = batch_df.filter(F.col("is_fraud") == True).select("tx_id", "reason", "risk_score")
    alerts.write.jdbc(PG_URL, "alerts", "append", PG_PROP)
    
    if batch_df.isStreaming or batch_df.storageLevel.useMemory:
        batch_df.unpersist()

# 8. Start Execution Engine and Wait for Incoming Signals
(df.writeStream.foreachBatch(write_batch)
   .outputMode("append")
   .option(
    "checkpointLocation",
    "C:/temp/fraud_checkpoint"
)
   .start()
   .awaitTermination())