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
CustomerCurated_node1701006153960 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="customer_curated",
    transformation_ctx="CustomerCurated_node1701006153960",
)

# Script generated for node Step Tainer Landing
StepTainerLanding_node1701001538921 = glueContext.create_dynamic_frame.from_catalog(
    database="stedi_db",
    table_name="step_trainer_landing",
    transformation_ctx="StepTainerLanding_node1701001538921",
)

# Script generated for node SQL Query Filter
SqlQuery881 = """
select s.* from step_trainer s join customer c on s.serialnumber=c.serialnumber

"""
SQLQueryFilter_node1701001591443 = sparkSqlQuery(
    glueContext,
    query=SqlQuery881,
    mapping={
        "step_trainer": StepTainerLanding_node1701001538921,
        "customer": CustomerCurated_node1701006153960,
    },
    transformation_ctx="SQLQueryFilter_node1701001591443",
)

# Script generated for node Step Trainer Trusted
StepTrainerTrusted_node1701001687590 = glueContext.getSink(
    path="s3://glue-s3-enock/step_trainer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    compression="gzip",
    enableUpdateCatalog=True,
    transformation_ctx="StepTrainerTrusted_node1701001687590",
)
StepTrainerTrusted_node1701001687590.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="step_trainer_trusted"
)
StepTrainerTrusted_node1701001687590.setFormat("json")
StepTrainerTrusted_node1701001687590.writeFrame(SQLQueryFilter_node1701001591443)
job.commit()
