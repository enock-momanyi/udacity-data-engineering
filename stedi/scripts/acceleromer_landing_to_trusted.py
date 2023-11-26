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

# Script generated for node Customer Trusted
CustomerTrusted_node1700997276982 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1700997276982",
)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1700997089245 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="accelerometer_landing",
    transformation_ctx="AccelerometerLanding_node1700997089245",
)

# Script generated for node SQL Query
SqlQuery735 = """
select a.* from accelerometer a join customer c on a.user=c.email
"""
SQLQuery_node1700997351893 = sparkSqlQuery(
    glueContext,
    query=SqlQuery735,
    mapping={
        "accelerometer": AccelerometerLanding_node1700997089245,
        "customer": CustomerTrusted_node1700997276982,
    },
    transformation_ctx="SQLQuery_node1700997351893",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1700997469208 = glueContext.getSink(
    path="s3://glue-s3-enock/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1700997469208",
)
AccelerometerTrusted_node1700997469208.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1700997469208.setFormat("json")
AccelerometerTrusted_node1700997469208.writeFrame(SQLQuery_node1700997351893)
job.commit()
