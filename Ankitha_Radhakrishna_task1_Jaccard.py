#import findspark
#findspark.init()
import sys
import pyspark
import time

sc = pyspark.SparkContext()

start = time.time()
inputFile = sys.argv[1]
#inputFile = "E:\\USC\\DataMining\\Assignment\\Assignment2\\Assignment_02\\Description\\Data\\Original\\Small1.csv"
#inputFile = "E:\\USC\\DataMining\\Assignment\\Assignment2\\Assignment_02\\Description\\Data\\Small1.csv"
#inputFile = "ratings.csv"
fileContents = sc.textFile(inputFile)
RDD = fileContents.zipWithIndex().filter(lambda (row,index): index > 0).keys().map(lambda x:x.split(','))
charMatRaw = RDD.map(lambda x : (int(x[0]),int(x[1])))

#charMatRaw2 = RDD.map(lambda x : (int(x[0]),[int(x[1])])).reduceByKey(lambda a,b:a+b).sortByKey()

users = charMatRaw.groupByKey().sortByKey().map(lambda x : x[0])
usersList = users.collect()
#print "usersList"
#print usersList

numOfHashFunctions = 12
ROWS = 2

#a = [1,1,1,1]
#b = [1,2,3,4]
a = [1,3,5,7,9,27,13,15,17,19,21,23]
b = [1,2,3,4,5,6,7,8,9,10,11,12]
#a = [1,3,5,7,9,11,13,15,17,19,21,23]
#b = [5,10,15,20,25,30,35,40,45,50,55,60]

num = len(usersList)

listOfHashFunctions = []
for i in range(numOfHashFunctions):
    tempF = users.map(lambda x : ((a[i] * x + b[i])%671)).collect()
    listOfHashFunctions.append(tempF)
 
#print "size of listOfHashFunctions = %d" % len(listOfHashFunctions)

rddMovie = RDD.map(lambda x : (int(x[1]),[int(x[0])])).reduceByKey(lambda a,b:a+b).sortByKey()
#print "rddMovie.collect"
#print rddMovie.collect()


dic2 = dict()
for i in usersList:
    dic2[i] = []



for i in listOfHashFunctions:
    #print "i"
    #print i
    k = 1
    for j in i:
        dic2[k].append(j)
        k += 1
        
#print "dic2"
#print dic2
        
def findMinValuesFromDic(ls,fNo):
    ls2 = []
    for j in ls:
        x = dic2[j][fNo]
        ls2.append(x)
    return ls2
        
def findMin(ls):
    min = ls[0]
    for i in ls[0:]:
        if i<min:
            min = i
    return min



#print dic2




s0 = rddMovie.map(lambda x : (x[0],findMinValuesFromDic(list(x[1]),0))).map(lambda x : (x[0],findMin(x[1])))
s1 = rddMovie.map(lambda x : (x[0],findMinValuesFromDic(list(x[1]),1))).map(lambda x : (x[0],findMin(x[1])))
s2 = rddMovie.map(lambda x : (x[0],findMinValuesFromDic(list(x[1]),2))).map(lambda x : (x[0],findMin(x[1])))
s3 = rddMovie.map(lambda x : (x[0],findMinValuesFromDic(list(x[1]),3))).map(lambda x : (x[0],findMin(x[1])))
s4 = rddMovie.map(lambda x : (x[0],findMinValuesFromDic(list(x[1]),4))).map(lambda x : (x[0],findMin(x[1])))
s5 = rddMovie.map(lambda x : (x[0],findMinValuesFromDic(list(x[1]),5))).map(lambda x : (x[0],findMin(x[1])))
s6 = rddMovie.map(lambda x : (x[0],findMinValuesFromDic(list(x[1]),6))).map(lambda x : (x[0],findMin(x[1])))
s7 = rddMovie.map(lambda x : (x[0],findMinValuesFromDic(list(x[1]),7))).map(lambda x : (x[0],findMin(x[1])))
s8 = rddMovie.map(lambda x : (x[0],findMinValuesFromDic(list(x[1]),8))).map(lambda x : (x[0],findMin(x[1])))
s9 = rddMovie.map(lambda x : (x[0],findMinValuesFromDic(list(x[1]),9))).map(lambda x : (x[0],findMin(x[1])))
s10 = rddMovie.map(lambda x : (x[0],findMinValuesFromDic(list(x[1]),10))).map(lambda x : (x[0],findMin(x[1])))
s11 = rddMovie.map(lambda x : (x[0],findMinValuesFromDic(list(x[1]),11))).map(lambda x : (x[0],findMin(x[1])))

#print s0.collect()
#print s1.collect()
#print s2.collect()
#print s3.collect()




j1 = s0.union(s1).groupByKey().map(lambda x:(x[0],tuple(x[1])))
j2 = j1.union(s2).groupByKey().map(lambda x:(x[0],tuple(x[1]))).mapValues(lambda x: x[0] + (x[1], ))
j3 = j2.union(s3).groupByKey().map(lambda x:(x[0],tuple(x[1]))).mapValues(lambda x: x[0] + (x[1], ))
j4 = j3.union(s4).groupByKey().map(lambda x:(x[0],tuple(x[1]))).mapValues(lambda x: x[0] + (x[1], ))
j5 = j4.union(s5).groupByKey().map(lambda x:(x[0],tuple(x[1]))).mapValues(lambda x: x[0] + (x[1], ))
j6 = j5.union(s6).groupByKey().map(lambda x:(x[0],tuple(x[1]))).mapValues(lambda x: x[0] + (x[1], ))
j7 = j6.union(s7).groupByKey().map(lambda x:(x[0],tuple(x[1]))).mapValues(lambda x: x[0] + (x[1], ))
j8 = j7.union(s8).groupByKey().map(lambda x:(x[0],tuple(x[1]))).mapValues(lambda x: x[0] + (x[1], ))
j9 = j8.union(s9).groupByKey().map(lambda x:(x[0],tuple(x[1]))).mapValues(lambda x: x[0] + (x[1], ))
j10 = j9.union(s10).groupByKey().map(lambda x:(x[0],tuple(x[1]))).mapValues(lambda x: x[0] + (x[1], ))
j11 = j10.union(s11).groupByKey().map(lambda x:(x[0],tuple(x[1]))).mapValues(lambda x: x[0] + (x[1], ))


signMat = j11.sortByKey().collect()
#print signMat


candidatePairsList = []
lenOfRes = len(signMat)

def returnIndex(a,b):
    band = b * 2
    indexNum = a + band
    return indexNum

for i in range(lenOfRes - 1):
    #Adding to dictionary
    #dicSignatures[signMat[i][0]] = signMat[i][1]
    for j in range(i+1,lenOfRes):
        l1 = list(signMat[i][1])
        l2 = list(signMat[j][1])
        numOfBands = len(l1)/ROWS
        k = 0
        while k<numOfBands:
        #for k in range(numOfBands):
            check = 0
            l = 0
            while l<ROWS:
            #for l in range(ROWS):
                ind = returnIndex(l,k)
                if l1[ind] == l2[ind]:
                    check += 1
                else:
                    break
                l += 1
            if(check == ROWS):
                item1 = signMat[i][0]
                item2 = signMat[j][0]
                candidatePairsList.append((item1,item2))
                break
            k+=1
                
#Adding last element to list
#dicSignatures[signMat[lenOfRes - 1][0]] = signMat[lenOfRes - 1][1]
#print "candidatePairsList"
#print candidatePairsList
#print len(candidatePairsList)
#print "========================================================"


# In[36]:


finalAnswer = []
checkGT = []
MoviesUsersDict = rddMovie.collectAsMap()

for pair in candidatePairsList:
    #print "pair"
    #print pair
    set1 = set(MoviesUsersDict[pair[0]])
    #print "set1"
    #print set1
    set2 = set(MoviesUsersDict[pair[1]])
    #print "set2"
    #print set2
    jaccardSim = float(len(set1 & set2))/len(set1 | set2)
    if jaccardSim >= 0.5:
        tupleAns = (pair[0],pair[1],jaccardSim)
        finalAnswer.append(tupleAns)
        checkGT.append((pair[0],pair[1]))
#print "finalAnswer"
#print len(finalAnswer)

#print finalAnswer

finalStr = ""
for i in finalAnswer:
    strI = str(i)
    strJ = strI[1:-1]
    finalStr += strJ+"\n"

fileName = "Ankitha_Radhakrishna_SimilarMovies_Jaccard.txt"
f = open(fileName,'w')
f.write("%s" % finalStr)
f.close()

end = time.time()
timeTaken = end-start

print "Time = "+str(timeTaken)
'''
inputFileGT = "SimilarMovies.GroundTruth.05.csv"
#inputFile = "E:\\USC\\DataMining\\Assignment\\Assignment2\\Assignment_02\\Description\\Data\\Small1.csv"
#inputFile = "ratings.csv"
fileContentsGT = sc.textFile(inputFileGT)
RDDGT = fileContentsGT.map(lambda x:x.split(','))
GT = RDDGT.map(lambda x : (int(x[0]),int(x[1])))


GTSet = set(GT.collect())

FinalPairsSet = set(checkGT)
falseNegatives = GTSet - FinalPairsSet
truePositives = GTSet.intersection(FinalPairsSet)
falsePositives = FinalPairsSet - GTSet

truePositiveslen = len(truePositives)
falsePositiveslen = len(falsePositives)
falseNegativeslen = len(falseNegatives)


print "TruePositives"
print truePositiveslen
print "falsePositives"
print falsePositiveslen
print "falseNegatives"
print falseNegativeslen

recall = float(truePositiveslen)/(truePositiveslen + falseNegativeslen)
print "recall"+str(recall)

precision = float(truePositiveslen)/(truePositiveslen + falsePositiveslen)
print "precision"+str(precision)
'''

