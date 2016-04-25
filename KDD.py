
from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression
from pyspark.ml.feature import HashingTF, Tokenizer
from pyspark.sql import Row
from pyspark.sql.functions import UserDefinedFunction
from pyspark.sql.types import *

def csvParse(s):
    import csv
    from StringIO import StringIO
    sio = StringIO(s)
    value = csv.reader(sio, delimiter="\t").next()
    sio.close()
    return value

blob1='wasb:///mag/PaperUrls.txt'
blob2='wasb:///mag/Papers.txt'
blob3='wasb:///mag/PaperAuthorAffiliations.txt'
inspections1 = sc.textFile(blob1).map(csvParse)
inspections2 = sc.textFile(blob2).map(csvParse)
inspections3 = sc.textFile(blob3).map(csvParse)

print(inspections1.take(2))
print(inspections2.take(2))
print(inspections3.take(2))






