
#import findspark
#findspark.init()

import sys
import pyspark
import time
import math

start = time.time()
sc = pyspark.SparkContext()

inputFile = sys.argv[1]
#inputFile = "E:\\USC\\DataMining\\Assignment\\Assignment3\\Assignment_03\\Description\\data\\ratings.csv"
#inputFile = "ratings.csv"
#inputFile = "E:\\USC\\DataMining\\Assignment\\Assignment2\\Assignment_02\\Description\\Data\\Small1.csv"
#inputFile = "ratings.csv"
fileContents = sc.textFile(inputFile)
RDD = fileContents.zipWithIndex().filter(lambda (row,index): index > 0).keys().map(lambda x:x.split(','))
originalRDD = RDD.map(lambda x : ((int(x[0]),int(x[1])),float(x[2])))
#originalRDD.collect()

inputFileTest = sys.argv[2]
#inputFileTest = "E:\\USC\\DataMining\\Assignment\\Assignment3\\Assignment_03\\Description\\data\\testing_small.csv"
#inputFileTest = "testing_small.csv"
#inputFile = "E:\\USC\\DataMining\\Assignment\\Assignment2\\Assignment_02\\Description\\Data\\Small1.csv"
#inputFile = "ratings.csv"
fileContentsTest = sc.textFile(inputFileTest)
RDDTest = fileContentsTest.zipWithIndex().filter(lambda (row,index): index > 0).keys().map(lambda x:x.split(','))
testRDD = RDDTest.map(lambda x : ((int(x[0]),int(x[1])),1.0))
testList =  testRDD.collect()

#print testList

trainingRDD = originalRDD.subtractByKey(testRDD)
test1 = trainingRDD.map(lambda x:(x[0][0],(x[0][1],x[1]))).groupByKey().map(lambda x : (x[0],dict(x[1])))
Test1List = sorted(test1.collect())

movieToUsers = trainingRDD.map(lambda x:(x[0][0],x[0][1],x[1])).map(lambda x : (x[1],x[0])).groupByKey()
movieToUsersDict = movieToUsers.mapValues(list).collectAsMap()

userToMovie = trainingRDD.map(lambda x:(x[0][0],x[0][1],x[1])).map(lambda x : (x[0],x[1])).groupByKey().map(lambda x : (x[0],len(list(x[1]))))
userToMovieList = userToMovie.collect()



#print movieToUsersDict

def computeWeight(listRatingsU,listRatingsV,rAvgU,rAvgV):
    NR = 0.0
    DR1 = 0.0
    DR2 = 0.0
    for i in range(len(listRatingsU)):
        Ru = listRatingsU[i] - rAvgU
        Rv = listRatingsV[i] - rAvgV
        NR += (Ru * Rv)
        DR1 += (Ru * Ru)
        DR2 += (Rv * Rv)
    sqrt1 = math.sqrt(DR1)
    sqrt2 = math.sqrt(DR2)
    DR = sqrt1 * sqrt2
    if DR == 0:
        return 0.0
    else:
        sim = float(NR)/DR
        return sim

simDict = dict()
for u in range(len(Test1List)-1):
    #print "u = "+str(Test1List[u][0])
    userU = Test1List[u][0]
    DictU = Test1List[u][1]
    #print DictU
    for v in range(u+1,len(Test1List)):
        userV = Test1List[v][0]
        tupleUV = (userU,userV)
        #print "v = "+str(Test1List[v][0])
        DictV = Test1List[v][1]
        #print DictV
        m1Set = set(DictU.keys())
        m2Set = set(DictV.keys())
        #print m1Set
        #print m2Set
       
        coRated = m1Set & m2Set
        #print coRated
        length = len(coRated)
        #print length
        #print "==============================================================="
        ls = list(coRated)
        if len(ls) != 0:
            rAvgU = 0
            rAvgV = 0
            listRatingsU = []
            listRatingsV = []
            for k in ls:
                #print "k = "+str(k)
                #print "DictU[k] = "+str(DictU[k])
                #print "DictV[k] = "+str(DictV[k])
                ratingsU = DictU[k]
                ratingsV = DictV[k]
                rAvgU += ratingsU
                rAvgV += ratingsV
                listRatingsU.append(ratingsU)
                listRatingsV.append(ratingsV)
            #print listRatingsU
            #print listRatingsV
            rAvgU = rAvgU/length
            rAvgV = rAvgV/length
            #print rAvgU
            #print rAvgV
            wtUV = computeWeight(listRatingsU,listRatingsV,rAvgU,rAvgV) 
            
            #Looping through all ratings of u and v for prediction calculation
            PSumU = 0.0
            PSumV = 0.0
            PLenU = 0
            PLenV = 0
            lsMoviesU = list(m1Set)
            lsMoviesV = list(m2Set)
            for i in lsMoviesU:
                PSumU += DictU[i]
                PLenU += 1
                
            for j in lsMoviesV:
                PSumV += DictV[j]
                PLenV += 1
            
            #print "wtUV = "+str(wtUV)
            simDict[tupleUV] = [wtUV,PSumU,PLenU,PSumV,PLenV]
            #print "=============================================================="
        else:
            
            simDict[tupleUV] = [0.0]

#print simDict
#print "*****************************************************************************"
#print "Dictionary of signatures ready"
#print "*****************************************************************************"

'''
fileName = "Ankitha_Radhakrishna_UserBasedCF.txt"
f = open(fileName,'w')
f.write("%s" % str(simDict))
f.close()
'''

Test1Dict = dict(Test1List)
testRDDForPrediction = testRDD.map(lambda x : x[0])
testRDDForPredictionList = testRDDForPrediction.collect()

#print Test1Dict

def maintainUsersOrder(a,b):
    if a<b:
        return (a,b)
    else:
        return (b,a)


def computePrediction(user,rddUserMovieList,PRUAvg):
    DR = 0.0
    NR = 0.0
    for j in rddUserMovieList:
        #print "j = "+str(j)
        TupleUV = maintainUsersOrder(user,j)
        #print TupleUV
        wtUV = simDict[TupleUV]
        #print "wtUV"
        #print wtUV
        weightUV = wtUV[0]
        #print "weight = "+str(weightUV)
        if weightUV != 0.0:
            if TupleUV.index(j) == 0:
                sumV = wtUV[1]
                lenV = wtUV[2]
            else:
                sumV = wtUV[3]
                lenV = wtUV[4]
            #print "sumV = "+str(sumV)
            #print "lenV = "+str(lenV)
        
            ratingVM = Test1Dict[j][movie]
            #print "rating of j "+str(ratingVM)
            sumV = sumV - ratingVM
            lenV = lenV - 1
            #print "sumV = "+str(sumV)
            #print "lenV = "+str(lenV)
            PRVAvg = sumV/lenV
            #print "PRVAvg = "+str(PRVAvg)
            NRTerm1 = ratingVM - PRVAvg
            NRTerm2 = NRTerm1 * weightUV
            #print "NRTerm2 = "
            #print NRTerm2
            NR += NRTerm2
            DR += abs(weightUV)
    #print "DR = "+str(DR)
    #print "NR = "+str(NR)
    Term = 0.0
    if DR != 0:
        Term = NR/DR
    else:
        Term = 0.0
    PUV = PRUAvg + Term
    #print "PUV = "+str(PUV)
    return PUV

predictions = []
userUAvg = dict()
#print rddUserMovieList
for i in testRDDForPredictionList:
    user = i[0]
    movie = i[1]
    '''
    print "*****************************************************************************"
    print "user = "+str(user)
    print "movie = "+str(movie)
    '''
    #rddUserMovie = trainingRDD.map(lambda x : (x[0][0],x[0][1],x[1])).map(lambda x : (x[0],x[1])).groupByKey().filter(lambda x : x[0] != user).filter(lambda x : movie in list(x[1])).map(lambda x : x[0])
    #rddUserMovieList = rddUserMovie.collect()
    #print "Created RDD for other users"
    #print "rddUserMovieList = "
    #print rddUserMovieList
    if movie in movieToUsersDict.keys():
        rddUserMovieList = movieToUsersDict[movie]
    else:
        rddUserMovieList = []
    PRUAvg = 0.0
    if user in userUAvg.keys():
        PRUAvg = userUAvg[user]     
    else:
        dictU = Test1Dict[user]
        sumOfRating = 0.0
        lengthOfRating = 0
        for m in dictU.keys():
            sumOfRating += dictU[m]
            lengthOfRating += 1
        PRUAvg = float(sumOfRating)/lengthOfRating
        userUAvg[user]=PRUAvg
    if len(rddUserMovieList) != 0:
        #print "Calling compute Prediction"
        pred = computePrediction(user,rddUserMovieList,PRUAvg)
        #print "Returned from prediction"
    else:
        pred = PRUAvg
        
        #if pred > 5:
        #   pred = 5
        #elif pred < 0:
        #    pred = 0
    predictions.append((user,movie,pred))
            
fileName = "Ankitha_Radhakrishna_UserBasedCF.txt"
f2 = open(fileName,'w')
str1 = ""
for i in predictions:
    str2 = str(i)
    str3 = str2[1:-1]
    str1 += str3+"\n"

f2.write("%s" % str1)

end = time.time()
#print "time taken = "
#print end-start
timeTaken = end - start

predictionsRDD = sc.parallelize(predictions).map(lambda x: ((x[0],x[1]),x[2]))
'''
print "************************************************************************"
print predictionsRDD.collect()
print "************************************************************************"
'''
ratesAndPreds = originalRDD.join(predictionsRDD)

'''
print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
print ratesAndPreds.collect()
print "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++"
'''

MSE = ratesAndPreds.map(lambda r: (r[1][0] - r[1][1])**2).mean()
#print("Mean Squared Error = " + str(MSE))

EMSE = math.sqrt(MSE)
#print "EMSE = "+str(EMSE)

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
    if diff >= 4 and diff < 5:
        level5 += 1

print ">=0 and <1: %d" % level1
print ">=1 and <2: %d" % level2
print ">=2 and <3: %d" % level3
print ">=3 and <4: %d" % level4
print ">=4: %d" % level5
print "RMSE = "+str(EMSE)
print "Time: "+str(timeTaken)
