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

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1701002545619 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="accelerometer_trusted",
    transformation_ctx="AccelerometerTrusted_node1701002545619",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1701002524169 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer_trusted",
    transformation_ctx="CustomerTrusted_node1701002524169",
)

# Script generated for node SQL Query JOIN
SqlQuery771 = """
select distinct c.serialnumber, c.customername,c.email,c.phone,c.birthday,c.registrationdate,c.sharewithpublicasofdate,c.sharewithresearchasofdate,c.sharewithfriendsasofdate from customer c join accelerometer a on c.email=a.user
"""
SQLQueryJOIN_node1701002572457 = sparkSqlQuery(
    glueContext,
    query=SqlQuery771,
    mapping={
        "accelerometer": AccelerometerTrusted_node1701002545619,
        "customer": CustomerTrusted_node1701002524169,
    },
    transformation_ctx="SQLQueryJOIN_node1701002572457",
)

# Script generated for node Customer Curated
CustomerCurated_node1701002841925 = glueContext.getSink(
    path="s3://glue-s3-enock/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="CustomerCurated_node1701002841925",
)
CustomerCurated_node1701002841925.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="customer_curated"
)
CustomerCurated_node1701002841925.setFormat("json")
CustomerCurated_node1701002841925.writeFrame(SQLQueryJOIN_node1701002572457)
job.commit()
