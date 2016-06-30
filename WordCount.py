
#Author: Nicolas Lopez

from pyspark import SparkContext as sc

"""
Short program that takes in a list of words, breaks up the list and distribute it accross a number of nodes
Then the program counts and returns the number of unique words in the list

The function that does most of the work is the 'reduceByKey' pyspark function

Example:  input_list = ['run','sit','dog','cat','run','dog','cat','sit','run','fun']
Will return: 5  (there are 4 unique words in this list)

"""

def countUniqueWords(word_list,nodes):
  
  wordListRDD = sc.parallelize(word_list,nodes) # Create a Resiliant Distributed Dataset object (RDD) from a given list, across # of nodes 
  
  wordValuesRDD = wordValues(wordListRDD) #A tuple consisting of each word and the number 1. 
  
  uniqueWordsRDD = wordValuesRDD.reduceByKey(lambda x,y: x+y) # Does pairwise addition of number of uniqe words after grouping them by word
  
  total_number_unique_words = uniqueWordsRDD.count()
  
  return total_number_unique_words

  
def wordValues(wordRDD):
  pairsRDD = wordRDD.map(lambda x:(x,1) )  # Creates a tuple of each word in word_RDD along with the values 1.  
  
  return pairsRDD
  
