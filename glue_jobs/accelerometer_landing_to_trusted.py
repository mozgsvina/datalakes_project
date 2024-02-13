import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ["JOB_NAME"])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args["JOB_NAME"], args)

# Script generated for node Accelerometer Landing
AccelerometerLanding_node1707739822763 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lakehouse-anya/accelerometer/landing/"],
        "recurse": True,
    },
    transformation_ctx="AccelerometerLanding_node1707739822763",
)

# Script generated for node Customer Trusted
CustomerTrusted_node1707748316231 = glueContext.create_dynamic_frame.from_options(
    format_options={"multiline": False},
    connection_type="s3",
    format="json",
    connection_options={
        "paths": ["s3://lakehouse-anya/customer/trusted/"],
        "recurse": True,
    },
    transformation_ctx="CustomerTrusted_node1707748316231",
)

# Script generated for node Join Customer
JoinCustomer_node1707748361931 = Join.apply(
    frame1=AccelerometerLanding_node1707739822763,
    frame2=CustomerTrusted_node1707748316231,
    keys1=["user"],
    keys2=["email"],
    transformation_ctx="JoinCustomer_node1707748361931",
)

# this affects the results so i didnt use it to get screenshots
# change frame in the next step to implement filtering

# # Script generated for node Filter By Research Consent Date
# SqlQuery0 = """
# select * from data
# WHERE shareWithResearchAsOfDate <= timestamp
# """
# FilterByResearchConsentDate_node1707825262371 = sparkSqlQuery(
#     glueContext,
#     query=SqlQuery0,
#     mapping={"data": JoinCustomer_node1707748361931},
#     transformation_ctx="FilterByResearchConsentDate_node1707825262371",
# )

# Script generated for node Drop Fields
DropFields_node1707748399328 = DropFields.apply(
    frame=JoinCustomer_node1707748361931,
    paths=[
        "serialNumber",
        "shareWithPublicAsOfDate",
        "birthDay",
        "registrationDate",
        "shareWithResearchAsOfDate",
        "customerName",
        "shareWithFriendsAsOfDate",
        "email",
        "lastUpdateDate",
        "phone",
    ],
    transformation_ctx="DropFields_node1707748399328",
)

# Script generated for node Accelerometer Trusted
AccelerometerTrusted_node1707748474596 = glueContext.getSink(
    path="s3://lakehouse-anya/accelerometer/trusted/",
    connection_type="s3",
    updateBehavior="UPDATE_IN_DATABASE",
    partitionKeys=[],
    enableUpdateCatalog=True,
    transformation_ctx="AccelerometerTrusted_node1707748474596",
)
AccelerometerTrusted_node1707748474596.setCatalogInfo(
    catalogDatabase="stedi_db", catalogTableName="accelerometer_trusted"
)
AccelerometerTrusted_node1707748474596.setFormat("json")
AccelerometerTrusted_node1707748474596.writeFrame(DropFields_node1707748399328)
job.commit()
