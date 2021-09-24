                                                                                                              
from pyspark import SparkContext                                                                                        
from pyspark.sql import SparkSession                                                                                    
from pyspark.streaming import StreamingContext                                                                          
from pyspark.streaming.kafka import KafkaUtils  
import json 
import pandas as pd  
import pickle 
import sys
import os


sc = SparkContext(appName="Something")                                                                                     
ssc = StreamingContext(sc, 60)                                                                                       
                                                                                                               
ks = KafkaUtils.createDirectStream(ssc, ['houseprice'], {'metadata.broker.list': 'localhost:9092'})                       
                                                                                                           
lines = ks.map(lambda x: json.loads(x[1]))  
#transform = lines.map(lambda data: predict(data))
#transform = lines.map(lambda feature: (feature, int(len(feature.split()))))  
lines.pprint()                                                                                        
    
                                                                                                                   
ssc.start()                                                                                                             
ssc.awaitTermination()


# function to save the model
def save_model(model):
    #if not os.path.isfile("model.pkl"):
        pickle.dump(model, open("model.pkl", "wb"))

# function to load the model
def load_model():
    #if os.path.isfile("model.pkl"):
        model = pickle.load(open("model.pkl", "rb"))
        return model

# # function to train and save the model as part of the feedback loop
# def train_model(data):
#     # load the model
#     clf = pickle.load(open("model.pkl", "rb"))

#     # pull out the relevant X and y from the FeedbackIn object
#     #X = [list(d.dict().values())[:-1] for d in data]
#     y = [r_classes[d.flower_class] for d in data]

#     # fit the classifier again based on the new data obtained
#     clf.fit(X, y)

#     # save the model
#     pickle.dump(clf, open("model.pkl", "wb"))






                                                                                                   

