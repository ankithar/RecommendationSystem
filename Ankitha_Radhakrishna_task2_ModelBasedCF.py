#import findspark
#findspark.init()

import sys
import pyspark
import time
import math

from pyspark.mllib.recommendation import ALS, MatrixFactorizationModel, Rating

sc = pyspark.SparkContext()

start = time.time()
inputFileTraining = sys.argv[1]
#inputFileTraining = "E:\\USC\\DataMining\\Assignment\\Assignment3\\Assignment_03\\Description\\data\\ratings.csv"
#inputFileTraining = "ratings.csv"
fileContentsTraining = sc.textFile(inputFileTraining)
RDDTrainingInter = fileContentsTraining.zipWithIndex().filter(lambda (row,index): index > 0).keys().map(lambda x:x.split(','))
RDDTraining = RDDTrainingInter.map(lambda x : ((int(x[0]),int(x[1])),float(x[2])))
#Try
#RDDTraining = RDDTrainingInter.map(lambda x : (int(x[0]),int(x[1]),float(x[2])))
#print "len of RDDTraining"
#print len(RDDTraining.collect())
#RDDTraining.take(10)

#inputFileTesting = "E:\\USC\\DataMining\\Assignment\\Assignment3\\Assignment_03\\Description\\data\\testing_small.csv"
#inputFileTesting = "testing_small.csv"
inputFileTesting = sys.argv[2]
fileContentsTesting = sc.textFile(inputFileTesting)
RDDTestingInter = fileContentsTesting.zipWithIndex().filter(lambda (row,index): index > 0).keys().map(lambda x:x.split(','))
RDDTesting = RDDTestingInter.map(lambda x : ((int(x[0]),int(x[1])),1))
#Try
#RDDTesting = RDDTestingInter.map(lambda x : int(x[0]),int(x[1]))
#print "len of RDDTesting"
#print len(RDDTesting.collect())
#RDDTesting.take(10)

RDDTrainingUpdated = RDDTraining.subtractByKey(RDDTesting)
#print "len of RDDTrainingUpdated"
#print len(RDDTrainingUpdated.collect())
#print "RDDTrainingUpdated collect"
#print RDDTrainingUpdated.collect()
#RDDTrainingUpdated.take(10)

ratings = RDDTrainingUpdated.map(lambda M :(list(M[0]),M[1])).map(lambda N : Rating(N[0][0],N[0][1],N[1]))
#ratings.take(5)

# Build the recommendation model using Alternating Least Squares
rank = 10
numIterations = 10
lambdaVal = 0.1
#model = ALS.train(ratings, rank, numIterations,lambdaVal)
model = ALS.train(ratings, rank, numIterations)


# Evaluate the model on training data
testdata = RDDTesting.map(lambda p: p[0])
predictions = model.predictAll(testdata).map(lambda r: ((r[0], r[1]), r[2]))
ratesAndPreds = RDDTraining.join(predictions)
MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
#print("Mean Squared Error = " + str(MSE))

EMSE = math.sqrt(MSE)



printRDD = predictions.map(lambda x : (x[0][0],x[0][1],x[1])).collect()
printRDDSorted = sorted(printRDD)
finalStr = ""
for i in printRDDSorted:
    strI = str(i)
    strJ = strI[1:-1]
    finalStr += strJ+"\n"

	
fileName = "Ankitha_Radhakrishna_ModelBasedCF.txt"
#if inputFileTesting.find("testing_small.csv") != -1:
#	fileName = "Ankitha_Radhakrishna_ModelBasedCF_Small.txt"
#elif inputFileTesting.find("testing_20m.csv") != -1:
#	fileName = "Ankitha_Radhakrishna_ModelBasedCF_Big.txt"

f = open(fileName,'w')
f.write("%s" % finalStr)
f.close()

end = time.time()
timeTaken = end - start

RatingTuple = ratesAndPreds.map(lambda x : x[1])

level1 = level2 = level3 = level4 = level5 = 0
RatingTupleList = RatingTuple.collect()
for tupleRate in RatingTupleList:
    #print tupleRate
    actual = tupleRate[0]
    predicted = tupleRate[1]
    #print "actual = %f" % actual
    #print "predicted = %f" % predicted
    diff = abs(actual - predicted)
    #print "diff = %f" % diff
    if diff >= 0 and diff < 1:
        level1 += 1
    if diff >= 1 and diff < 2:
        level2 += 1
    if diff >= 2 and diff < 3:
        level3 += 1
    if diff >= 3 and diff < 4:
        level4 += 1
    if diff >= 4:
        level5 += 1
        
print ">=0 and <1: %d" % level1
print ">=1 and <2: %d" % level2
print ">=2 and <3: %d" % level3
print ">=3 and <4: %d" % level4
print ">=4: %d" % level5
print "RMSE = "+str(EMSE)
print "Time: "+str(timeTaken)
    
