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
CustomerTrusted_node1707814895089 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lakehouse-anya/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1707814895089",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1707815105543 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lakehouse-anya/accelerometer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerTrusted_node1707815105543",
)

# Script generated for node Customer Join
SqlQuery8 = """
SELECT DISTINCT ct.*
FROM ct
JOIN at ON ct.email = at.user
"""
CustomerJoin_node1707817238179 = sparkSqlQuery(
    glueContext,
    query=SqlQuery8,
    mapping={
        "ct": CustomerTrusted_node1707814895089,
        "at": AccelerometerTrusted_node1707815105543,
    },
    transformation_ctx="CustomerJoin_node1707817238179",
)

# Script generated for node Customers Curated
CustomersCurated_node1707815216959 = glueContext.getSink(
    path="s3://lakehouse-anya/customer/curated/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="CustomersCurated_node1707815216959",
)
CustomersCurated_node1707815216959.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="customer_curated"
)
CustomersCurated_node1707815216959.setFormat("json")
CustomersCurated_node1707815216959.writeFrame(CustomerJoin_node1707817238179)
job.commit()
