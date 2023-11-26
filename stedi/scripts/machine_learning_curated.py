import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue import DynamicFrame


def sparkSqlQuery(glueContext, query, mapping, transformation_ctx) -> DynamicFrame:
    for alias, frame in mapping.items():
        frame.toDF().createOrReplaceTempView(alias)
    result = spark.sql(query)
    return DynamicFrame.fromDF(result, glueContext, transformation_ctx)


args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Customer Curated
CustomerCurated_node1701006596800 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1701006596800",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1701020023863 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1701020023863",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1701006571332 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="step_trainer_trusted",
    transformation_ctx="StepTrainerTrusted_node1701006571332",
)

# Script generated for node SQL Query JOIN
SqlQuery738 = """
select s.sensorreadingtime,s.distancefromobject from a 
join s on a.serialnumber=s.serialnumber
"""
SQLQueryJOIN_node1701006625469 = sparkSqlQuery(
    glueContext,
    query=SqlQuery738,
    mapping={
        "a": CustomerCurated_node1701006596800,
        "s": StepTrainerTrusted_node1701006571332,
    },
    transformation_ctx="SQLQueryJOIN_node1701006625469",
)

# Script generated for node Join
Join_node1701020118073 = Join.apply(
    frame1=SQLQueryJOIN_node1701006625469,
    frame2=AccelerometerTrusted_node1701020023863,
    keys1=["sensorreadingtime"],
    keys2=["timestamp"],
    transformation_ctx="Join_node1701020118073",
)

# Script generated for node Drop Fields
DropFields_node1701020215627 = DropFields.apply(
    frame=Join_node1701020118073,
    paths=["user"],
    transformation_ctx="DropFields_node1701020215627",
)

# Script generated for node Machine Learning Curated
MachineLearningCurated_node1701006883387 = glueContext.getSink(
    path="s3://glue-s3-enock/machine_learning/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="MachineLearningCurated_node1701006883387",
)
MachineLearningCurated_node1701006883387.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="machine_learning_curated"
)
MachineLearningCurated_node1701006883387.setFormat("json")
MachineLearningCurated_node1701006883387.writeFrame(DropFields_node1701020215627)
job.commit()
