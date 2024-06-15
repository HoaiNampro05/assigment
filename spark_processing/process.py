from pyspark.sql import SparkSession
from pyspark.sql.functions import col, date_format, to_date
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
spark = SparkSession.builder.appName("Assigment").getOrCreate()
def convert(x):
    print(x)
    a=x.split('/')
    return a[2]+a[0].zfill(2)+a[1]
    
convert_date = udf(convert,StringType())

log_path='hdfs:///raw_zone/fact/activity/log.csv'
ds_de_path='hdfs:///raw_zone/fact/activity/danh_sach_sv_de.csv'

de_schema="id int, name string"
log_schema="id int, activity string, numberOfFile int, timestamp string"


log_df = spark.read.format("csv").option("header","false").schema(log_schema).load(log_path)
de_df = spark.read.format("csv").option("header","false").schema(de_schema).load(ds_de_path)

log_df=log_df.withColumn('newtimestamp',convert_date('timestamp'))
de_df.createOrReplaceTempView("danhsachde")
log_df.createOrReplaceTempView("log")
result=spark.sql("""
select log.newtimestamp,log.id,danhsachde.name,log.activity,sum(log.numberOfFile) as numberOfFile
from log, danhsachde
where log.id = danhsachde.id
group by log.newtimestamp,log.id,danhsachde.name,log.activity
order by log.newtimestamp,log.id,danhsachde.name,log.activity
""")
output_path = "hdfs:///raw_zone/fact/activity/output"
result.write.mode("overwrite").csv(output_path)
