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

# Script generated for node Customer Landing
CustomerLanding_node1700992373048 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://glue-s3-enock/customer/landing/"],
        "recurse": True,
    },
    transformation_ctx="CustomerLanding_node1700992373048",
)

# Script generated for node SQL Query Filter
SqlQuery713 = """
select * from myDataSource
where sharewithresearchasofdate != 0
"""
SQLQueryFilter_node1700996029424 = sparkSqlQuery(
    glueContext,
    query=SqlQuery713,
    mapping={"myDataSource": CustomerLanding_node1700992373048},
    transformation_ctx="SQLQueryFilter_node1700996029424",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1700996112021 = glueContext.getSink(
    path="s3://glue-s3-enock/customer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="CustomerTrusted_node1700996112021",
)
CustomerTrusted_node1700996112021.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="customer_trusted"
)
CustomerTrusted_node1700996112021.setFormat("json")
CustomerTrusted_node1700996112021.writeFrame(SQLQueryFilter_node1700996029424)
job.commit()
