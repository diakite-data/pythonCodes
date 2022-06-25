from pyspark.sql import SparkSession
from pyspark.sql import Row
import argparse



# construct the argument parser and parse the arguments
ap = argparse.ArgumentParser()
ap.add_argument("-i", "--application", required=True,
	help="application name")
ap.add_argument("-e", "--date", required=True,
	help="date to enter")
#ap.add_argument("-d", "--detection-method", type=str, default="cnn",
#	help="face detection model to use: either `hog` or `cnn`")
args = vars(ap.parse_args())


spark = SparkSession \
    .builder \
    .appName(args["application"]) \
    .enableHiveSupport() \
    .getOrCreate() \
  #  .setLogLevel("ERROR")


spark.sparkContext.setLogLevel("ERROR")

log4jLogger = spark._jvm.org.apache.log4j
LOGGER = log4jLogger.LogManager.getLogger(__name__)

#LOGGER.getLogger("org").setLevel(LOGGER.Level.ERROR)
#LOGGER.getLogger("akka").setLevel(LOGGER.Level.ERROR)

LOGGER.info("=============================================================== INITIALISATION DU JOB ===============================================================")


query = "select * from espace_client_B2C.histo_souscription where eventdate >='%s'" % args["date"]

df = spark.sql(query)

#df.show()

q = "SELECT RT.START_TIME as DATE_EVENT, (CASE WHEN (length(RT.CALLING_NBR)=8) THEN RT.CALLING_NBR " \
    "WHEN (length(RT.CALLING_NBR)=11) THEN SUBSTR(RT.CALLING_NBR, 4,11) ELSE SUBSTR(RT.CALLING_NBR, 6,13) END) AS MSISDN, " \
    "RT.CALLED_NBR as APPELE, FR.OFFER_NAME, FR.OFFER_PROFILE_CAT, CASE RT.SERVICE_TYPE_LV WHEN 1 THEN 'DATA' " \
    "WHEN 2 THEN 'VOIX' WHEN 3 THEN 'SMS' WHEN 4 THEN 'USSD' WHEN 10 THEN 'IMS' END(char(50)) as CANAL, RT.DURATION " \
    "FROM PGANV_DWH.RATED_CDR_ALL RT INNER JOIN PGANV_DWH.RATABLE_EVENT RE ON (RE.RE_ID=RT.RE_ID) " \
    "INNER JOIN PGANV_DWH.OFFER FR ON (FR.DWH_OFFER_ID=RT.DWH_OFFER_ID) " \
    "WHERE CAST(START_TIME AS Date) BETWEEN date'2020-12-01' and date'2020-12-01' " \
    "AND RT.SERVICE_TYPE_LV IN (2,3)"


data_time = spark.read \
    .format("jdbc") \
    .option("url", "jdbc:teradata://10.241.10.5/GANDHI") \
    .option("driver", "com.teradata.jdbc.TeraDriver") \
    .option("user", "pganu_sel") \
    .option("password", "ocit") \
    .option("dbtable", q) \
    .load()

data_time.show()

#data_time.write.mode("overwrite").saveAsTable("data_time_python")



SystemExit('End of the job')
