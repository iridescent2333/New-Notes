from pyspark.sql import SparkSession

spark = SparkSession.bulider  \\

.appName("MyApp") \\

.getOrCreate()



sc = spark.sparkContext



使用的是pyspark，避免学习深入Scala，



mapreduce



import sys



def mapper(line):

line = line.strip()

if not line:

&#x09;return



fields = line.split(',')



key = fields\[N]

value = fields\[M]



if not value:

return



print("{}\\t{}".format(key,value))



def main():

&#x09;for line in sys.stdin:

mapper(line)



if \_\_name\_\_ == "\_\_name\_\_"":

&#x09;main()





def mapper(line):

&#x20;   fields = line.strip().split(',')

&#x20;   isbn = fields\[2]

&#x20;   borrow\_time = fields\[3]

&#x20;   return\_time = fields\[4]

&#x20;   if return\_time:                          # 还书时间不为空才计算

&#x20;       d = days(borrow\_time, return\_time)

&#x20;       print("{}\\t{}".format(isbn, d))





def reducer(key, values):

&#x20;   total = sum(int(v) for v in values)

&#x20;   print("{}\\t{}".format(key, total))







hadoop fs -put /root/books/recording1101.csv /user/library/books/

(hdfs dfs )



parallelize/map/reduce/filter



\*\*\*\*\*\*\*\*\*\*\*



(hadoop fs)

hdfs dfs -ls /user/library/

hdfs dfs -mkdir /user/library

hdfs dfs -mkdir -p /user/library/books

hdfs dfs -rm /user/library/1.csv

hdfs dfs -rm -r /user/library/backup/

hdfs dfs -cat /user/library/1.csv

hdfs dfs -tail /user/library/1.txt

hdfs dfs -mv /user/library/1.csv /user/library/2.csv

hdfs dfs -cp /user/library/1.txt /user/library/books/

hdfs dfs -du -h /user/library/



hdfs dfs -put /root/backup/1.csv /user/library/books/

(hdfs dfs -copyFromLocal /root/backup/1.csv /user/library/books/)

hdfs dfs -moveFromLocal/

hdfs dfs -copyToLocal/...

hdfs dfs -get /user/library/books/1..csv /root/backup/

hdfs dfs -cp /user/library/books/1.txt /user/library/books/1.csv



hdfs dfs -mkdir -p /user/student/input/🤔

hdfs dfs -put /root/data/input.csv /user/student/input



mkdir -p /home/student/result/🤔

hdfs dfs -get /user/student/result/output.csv /home/student/result/



hdfs dfs -rm -r /user/student/result/😊



hdfs dfs -cat /user/student/1.csv |head -10🤣



hdfs dfs -put /root/backup /user/ 🤔



hdfs dfsadmin -safemode get🤣

hdfs dfsadmin -safemode leave



hbase shell

hbase(main):001:0>

list

create 'person' , 'info', 'data'

describe 'person'

alter 'person', 'address'

alter 'person', {NAME => 'address', METHOD =>'delete'}🤔

truncate 'person'

disable 'person'

enable 'person'

disable 'person'

drop 'person'

exists 'person'



put 'person', '1001', 'info:name', '张三'🤔

get 'person', '1001','info:name'



scan 'person', {STARTROW => '1001', STOPROW => '1003'}🤔



delete 'person', '1001', 'info:name'



exit



create 'student', 'info', 'course'

put 'student', '202601', 'info:name', '王小明'

put 'student', '202601', 'info:age', '21'

put 'student', '202602', 'info:name', '李小花'

put 'student', '202602', 'info:age', '20'



scan 'student', '202601'

scan 'student'



alter 'student', 'address'

deleteall 'student', '2026002'🤔



scan 'student', {STARTROW => '2026001', STOPROW => '2026005'}

count 'student'🤣



import sys



def  mapper(line):

&#x09;line = line.strip()

&#x09;if not line:

&#x09;	return

fields = line.split(',')

key = fields\[N]

value = fields\[M]

if not value:

&#x09;return

print('{}\\t{}'.format(key, value))



def main():

&#x09;for line in sys.stdin

&#x09;	mapper(line)

if \_\_name\_\_ == "\_\_name\_\_":

&#x09;main()







import sys

def reducer(key,value):

&#x09;result = sum(int(v) for v in values)

&#x09;result = len(values)

&#x09;result = max(int(v) for v in values)

&#x09;result = min(int(v) for v invalue)

&#x09;nums = \[int(v) for v in values]

&#x09;result = sum(nums) / len(nums)

def main()

&#x09;kvs ={}

&#x09;for line in sys.stdin:

&#x09;line = line.strip()

&#x09;for not line:

&#x09;	continue

&#x09;k, v = line.split('\\t')

&#x09;kvs.setdefault(k,\[]).append(v)

&#x20;for k, v in kvs..items()

&#x09;reducer(k, v)

if \_\_name\_\_\_ == "\_\_name\_\_":

&#x09;main()



def mapper(line):

&#x09;line = line.strip(line)

&#x09;if not line

&#x09;	return

&#x09;fields = line.split(',')

&#x09;isbn = fields\[2]

&#x09;borrow\_time = fields\[3]

&#x09;return\_time = fields\[4]

&#x09;if return\_time:

&#x09;	d = days(borrow\_time, keep\_time)

&#x09;	print('{}\\t{}'.format(isbn, d))





def reducer(key, values)

&#x09;total = sum(int(v) for v in values)

&#x09;print('{}\\t{}'.format(key, total))







def mapper(line):

&#x09;line = line.strip(line)

&#x09;if not line

&#x09;	return

&#x09;fields = line.split(',')

&#x09;user\_id = fields\[2]

&#x09;	print('{}\\t1'.format(user\_id))



def reducer(key,values):

&#x09;count = len(values)

&#x09;print('{}\\t{}'.format(key,count))







from pyspark.sql import SparkSession



spark = SparkSession.bulider \\

&#x09;.appName("MyApp") \\

&#x09;getOrCreate()



sc = spark.sparkContext



from pyspark import SparkContext

sc = SparkContext('local', 'MyApp')

rdd =sc.parallelize(\[1,2,3,4,5,6,7,8,9,0])

rdd= sc.textFile('/path/to/file.txt')

rdd.map(lambda x: x\*x)

rdd.reduce(lambda a,b: a+b)







