from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import * 
from pyspark.sql.types import *
import re
from datetime import datetime

#+---------------------------------+
# Khoi tao spark session & context
#+---------------------------------+

spark = SparkSession \
    .builder \
    .master('local') \
    .appName('Assignment01') \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.12:3.0.1') \
    .config('spark.mongodb.input.uri', 'mongodb://localhost/dep303_asm01') \
    .config('spark.mongodb.output.uri', 'mongodb://localhost/dep303_asm01') \
    .getOrCreate()

sc = spark.sparkContext

#+---------------------------------+
# 2. Đọc dữ liệu từ MongoDB với Spark
#+---------------------------------+

questions_schema = StructType([
    StructField("Id", IntegerType()),
    StructField("OwnerUserId", StringType()),
    StructField("CreationDate", StringType()),
    StructField("ClosedDate", StringType()),
    StructField("Score", IntegerType()),
    StructField("Title", StringType()),
    StructField("Body", StringType())
])

questions_raw = spark.read \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .option("spark.mongodb.input.database", "dep303_asm01") \
    .option("collection", "Questions") \
    .schema(questions_schema) \
    .load()

#+---------------------------------+
answers_schema = StructType([
    StructField("Id", IntegerType()),
    StructField("OwnerUserId", StringType()),
    StructField("CreationDate", StringType()),
    StructField("ParentId", IntegerType()),
    StructField("Score", IntegerType()),
    StructField("Body", StringType())
])

answers_raw = spark.read \
    .format('com.mongodb.spark.sql.DefaultSource') \
    .option("spark.mongodb.input.database", "dep303_asm01") \
    .option("collection", "Answers") \
    .schema(answers_schema) \
    .load()

#+---------------------------------+
# 3. Chuẩn hóa dữ liệu
#+---------------------------------+

questions_df = questions_raw \
    .withColumn('CreationDate', col('CreationDate').cast(DateType())) \
    .withColumn('ClosedDate', col('ClosedDate').cast(DateType())) \
    .withColumn('OwnerUserId', col('OwnerUserId').cast(IntegerType())) \

print("\n---QUESTIONS---")
# questions_raw.show(5)
questions_raw.printSchema()

print("---NORMALIZED QUESTIONS---")
# questions_df.show(5)
questions_df.printSchema()

###
answers_df = answers_raw \
    .withColumn('CreationDate', col('CreationDate').cast(DateType())) \
    .withColumn('OwnerUserId', col('OwnerUserId').cast(IntegerType())) \

print("---ANSWERS---")
# answers_raw.show(5)
answers_raw.printSchema()

print("---NORMALIZED ANSWERS---")
# answers_df.show(5)
answers_df.printSchema()

#+---------------------------------+
# Yêu cầu 1: Tính số lần xuất hiện của các ngôn ngữ lập trình
#+---------------------------------+

# khởi tạo udf
def extract_languages(string):
    lang_regex = r"Java|Python|C\+\+|C#|Go|Ruby|Javascript|PHP|HTML|CSS|SQL"
    if string:
        return re.findall(lang_regex, string)
    
extract_languages_udf = udf(extract_languages, returnType=ArrayType(StringType()))

# chọn Body, duỗi Body_extract, group & count
languages_df = questions_df \
    .select('Body') \
    .withColumn('Body_extract', extract_languages_udf(col('Body'))) \
    .withColumn('Programing Language', explode('Body_extract')) \
    .groupBy('Programing Language') \
    .agg(count('Programing Language').alias('Count'))

print("---YEU CAU 1---")
languages_df.show()

#+---------------------------------+
# Yêu cầu 2 : Tìm các domain được sử dụng nhiều nhất trong các câu hỏi
#+---------------------------------+

# khởi tạo udf
def extract_domain(string):
    domain_regex = r'http://(\S+)"'
    if string:
        results = re.findall(domain_regex, string)
        return [r.split('/')[0] for r in results]

extract_domain_udf = udf(extract_domain, returnType=ArrayType(StringType()))

# chọn Body, duỗi Body_extract, group & count
domain_df = questions_df \
    .select('Body') \
    .withColumn('Body_extract', extract_domain_udf(col('Body'))) \
    .withColumn('Domain', explode('Body_extract')) \
    .groupBy('Domain') \
    .agg(count('Domain').alias('Count')) \
    .orderBy(desc('Count'))

print("---YEU CAU 2---")
domain_df.show()

#+---------------------------------+
# Yêu cầu 3 : Tính tổng điểm của User theo từng ngày
#+---------------------------------+

score_of_users_ques_df = questions_df \
    .select(col('OwnerUserId'), \
            col('CreationDate'), \
            col('Score').alias('Score_ques'))

score_of_users_ans_df = answers_df \
    .select(col('OwnerUserId').alias('OwnerUserId_ans'), \
            col('CreationDate').alias('CreationDate_ans'), \
            col('Score').alias('Score_ans'))

condition_join = (score_of_users_ques_df.OwnerUserId == score_of_users_ans_df.OwnerUserId_ans) \
                & (score_of_users_ques_df.CreationDate == score_of_users_ans_df.CreationDate_ans)

score_of_users_df = score_of_users_ques_df.join(score_of_users_ans_df, condition_join, 'inner') \
    .withColumn('Score', col('Score_ques') + col('Score_ans')) \
    .select('OwnerUserId', 'CreationDate', 'Score')

score_of_users_df = score_of_users_df \
    .groupBy('OwnerUserId', 'CreationDate') \
    .agg(sum('Score').alias('Score')) \
    .sort(asc('CreationDate'))

# windowing
running_total_window = Window.partitionBy('OwnerUserId') \
    .orderBy('CreationDate') \
    .rowsBetween(Window.unboundedPreceding, Window.currentRow)

score_of_users_df = score_of_users_df \
    .withColumn("TotalScore", sum("Score").over(running_total_window))

print("---YEU CAU 3---")
score_of_users_df.show()

#+---------------------------------+
# Yêu cầu 4: Tính tổng số điểm mà User đạt được trong một khoảng thời gian
#+---------------------------------+

# chuyen dinh dang thoi-gian-dang-chuoi
START = '01-01-2008'
END = '01-01-2009'
START = datetime.strftime(datetime.strptime(START, '%d-%m-%Y'), '%Y-%m-%d')
END = datetime.strftime(datetime.strptime(END, '%d-%m-%Y'), '%Y-%m-%d')

# loc users trong khoang thoi gian
score_of_users_ques_df = questions_df \
    .select('OwnerUserId', 'CreationDate', 'Score') \
    .dropna() \
    .filter((col('CreationDate') >= START) & (col('CreationDate') <= END)) \
    .select('OwnerUserId', 'Score')

# gom nhom user va tinh tong score
score_of_users_ques_df = score_of_users_ques_df \
    .groupBy('OwnerUserId') \
    .agg(sum(col('Score')).alias('TotalScore')) \
    .orderBy(asc('OwnerUserId'))


print("---YEU CAU 4---")
score_of_users_ques_df.show()

#+---------------------------------+
# Yêu cầu 5: Tìm các câu hỏi có nhiều câu trả lời
#+---------------------------------+

id_ques_df = questions_df \
    .select(col('Id').alias('Id_ques')) \
    .dropna()

id_ans_df = answers_df \
    .select(col('Id').alias('Id_ans'), 'ParentId') \
    .dropna()

# join 2 bang va tinh tong answers theo id question
cond_join = id_ques_df.Id_ques == id_ans_df.ParentId

ques_have_ans_df = id_ques_df.join(id_ans_df, cond_join, 'inner') \
    .select('Id_ques', 'Id_ans') \
    .groupBy('Id_ques') \
    .agg(count('Id_ans').alias('Count_ans')) 

# loc question co nhieu hon 5 answer
good_ques_df = ques_have_ans_df \
    .filter(col('Count_ans') >= 5) \
    .select(col('Id_ques').alias('Good Question Id'), col('Count_ans').alias('Number of Answer')) \
    .orderBy(asc('Good Question Id'))

print("---YEU CAU 5---")
good_ques_df.show()

#+---------------------------------+
# (Nâng cao) Yêu cầu 6: Tìm các Active User
#+---------------------------------+
# Có nhiều hơn 50 câu trả lời hoặc tổng số điểm đạt được khi trả lời lớn hơn 500.
# Có nhiều hơn 5 câu trả lời ngay trong ngày câu hỏi được tạo.

# user thoa dieu kien 1,2
users_condition_1_2_df = answers_df \
    .select('Id', 'OwnerUserId', 'Score') \
    .dropna() \
    .groupBy('OwnerUserId') \
    .agg(count('Id').alias('Number of Answer'), sum('Score').alias('Total Score')) \
    .filter((col('Number of Answer') > 50) | (col('Total Score') > 500))

# user thoa dieu kien 3
users_ques_df = questions_df \
    .select('Id', col('OwnerUserId').alias('OwnerUserId_ques'), 'CreationDate') \
    .dropna()

users_ans_df = answers_df \
    .select('Id', 'OwnerUserId', 'CreationDate', 'ParentId') \
    .dropna()

cond_join = (users_ques_df.Id == users_ans_df.ParentId) \
            & (users_ques_df.CreationDate == users_ans_df.CreationDate)

users_condition_3_df = users_ques_df.join(users_ans_df, cond_join, 'inner') \
    .select(users_ans_df.Id, 'OwnerUserId') \
    .groupBy('OwnerUserId') \
    .agg(count(users_ans_df.Id).alias('Num_Answer in CreationDate')) \
    .filter(col('Num_Answer in CreationDate') > 5)

# gop lai
users_condition_3_rename_df = users_condition_3_df \
    .withColumnRenamed('OwnerUserId', 'renamed_OwnerUserId')

all_join = users_condition_1_2_df.OwnerUserId == users_condition_3_rename_df.renamed_OwnerUserId

active_users_df = users_condition_1_2_df.join(users_condition_3_rename_df, all_join, 'inner') \
    .select(col('OwnerUserId').alias('Active User'), \
            'Number of Answer', 'Total Score', 'Num_Answer in CreationDate')

print("---YEU CAU 6---")
# users_condition_1_2_df.show(10)
# users_condition_3_df.show(10)
active_users_df.show()


