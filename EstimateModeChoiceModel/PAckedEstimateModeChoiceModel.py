################### ESTIMATION PARAMETERS ##########################
EPOCHES = 300 # This is a maximum number, not the number required
BATCH_SIZE = 1024
number_of_layers = 2
layer_size = 16
learning_rate = 0.1
dropout = 0.5 #[0.2, 0.3, 0.4]
l2 = 0.01
train_file_path = r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\ProduceTravelTimesFromRoadNetwork\bin\Release\netcoreapp2.2\CombinedTrain.csv"
test_file_path = r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\ProduceTravelTimesFromRoadNetwork\bin\Release\netcoreapp2.2\CombinedTest.csv"
real_cell_trace_dir = r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\ConvertCellTraces\bin\Release\netcoreapp2.2"
################### \ESTIMATION PARAMETERS ##########################
import functools

import numpy as np
import functools
import os
os.environ['TF_CPP_MIN_LOG_LEVEL'] = '3' 
import tensorflow as tf 
from tensorflow import keras
import pandas as pd
import logging
tf.get_logger().setLevel(logging.ERROR)
tf.debugging.set_log_device_placement(False)
# Make numpy values easier to read.
np.set_printoptions(precision=3, suppress=True)

def pack(features, label):
  return tf.stack(list(features.values()), axis=-1), label

class PackNumericFeatures(object):
  def __init__(self, names):
    self.names = names

  def __call__(self, features, labels):
    numeric_freatures = [features.pop(name) for name in self.names]
    numeric_features = [tf.cast(feat, tf.float32) for feat in numeric_freatures]
    numeric_features = tf.stack(numeric_features, axis=-1)
    features['numeric'] = numeric_features
    return features, labels

class PackNumericFeaturesNoLabel(object):
  def __init__(self, names):
    self.names = names

  def __call__(self, features):
    numeric_freatures = [features.pop(name) for name in self.names]
    numeric_features = [tf.cast(feat, tf.float32) for feat in numeric_freatures]
    numeric_features = tf.stack(numeric_features, axis=-1)
    features['numeric'] = numeric_features
    return features
    
numeric_features = []
column_types = [tf.int32]
    
for i in range(288):
    name = 'Distance' + str(i)
    numeric_features.append(name)
    column_types.append(tf.float32)
for i in range(288):
    name = 'Active' + str(i)
    numeric_features.append(name)
    column_types.append(tf.float32)
for i in ['OriginPopulationDensity', 'OriginEmploymentDensity','OriginHouseholdDensity','DestinationPopulationDensity','DestinationEmploymentDensity','DestinationHouseholdDensity']:
    name = i
    numeric_features.append(name)
    column_types.append(tf.float32)
    
numeric_features.append('TripDistance')
column_types.append(tf.float32)

def get_dataset(file_path, columns, epochs, ResultColumn, batch_size, **kwargs):
    df = pd.read_csv(file_path, delimiter=',', header = 0)#, names = columns, dtype = dtypes)
    target = None
    if not (ResultColumn is None):
        target = df.pop(ResultColumn)
    # Remove extra columns
    to_delete = []
    for name, _ in df.iteritems():
        if not (name in columns):
            to_delete.append(name)
    for d in to_delete:
        df.pop(d)

    if ResultColumn is None:
        return tf.data.Dataset.from_tensor_slices(dict(df))
    else:
        return (target.values, tf.data.Dataset.from_tensor_slices((dict(df), target.values)), df)

def train_model():
    def create_confusion_matrix(predictions, labels):
        confusion_matrix = [[0,0,0],[0,0,0],[0,0,0]]
        labelIndex = 0
        for pred in predictions:
            for i in range(3):
                confusion_matrix[labels[labelIndex]][i] += pred[i]
            labelIndex += 1
        return confusion_matrix
    
    def print_matrix(name, matrix):
        print("Matrix: " + str(name))
        for row in matrix:
            for col in row:
                print(col, end=',')
            print()
    
    LABEL_COLUMN = 'Result'
    LABELS = [0, 1, 2]
    LABEL_COLUMN = 'Result'
    columns = ['Result']
    columns = columns + numeric_features
    LABELS = [0, 1, 2]
    class_weights = {
            0 : 1,
            1 : 1,
            2 : 1
        }
    
    train_labels, raw_train_data, train_description = get_dataset(train_file_path, columns, 10, LABEL_COLUMN, BATCH_SIZE)
    test_labels, raw_test_data, test_description = get_dataset(test_file_path, columns, 1, LABEL_COLUMN, 1024)
 
    def normalize_numeric_data(data, mean, min, delta):
          return (data - min) / delta
    
    packed_train_data = raw_train_data.map(PackNumericFeatures(numeric_features))
    packed_test_data = raw_test_data.map(PackNumericFeatures(numeric_features))
       
    normalize_dataset = pd.read_csv(train_file_path)
    normalize_dataset = normalize_dataset[numeric_features]
    desc = normalize_dataset.describe()
    MEAN = np.array(desc.T['mean'])
    MIN = np.array(desc.T['min'])
    DELTA = (desc.T['max'] - desc.T['min'])
    for i in range(len(DELTA)):
        if DELTA[i] < 1.0:
            DELTA[i] = 1.0
    DELTA = np.array(DELTA)  
    
    normalizer = functools.partial(normalize_numeric_data, mean=MEAN, min=MIN, delta=DELTA)
    numeric_column = tf.feature_column.numeric_column('numeric', normalizer_fn=normalizer, shape=[len(numeric_features)])
    numeric_columns = [numeric_column]

    print (len(DELTA))
    print (len(numeric_features))
        
    layers = [tf.keras.layers.DenseFeatures(numeric_columns)]
    for i in range(number_of_layers):
        layers.append(tf.keras.layers.Dense(layer_size, activation='relu', kernel_regularizer=tf.keras.regularizers.l2(l2)))
        layers.append(tf.keras.layers.Dropout(dropout))
    layers.append(keras.layers.Dense(3, activation='softmax', kernel_regularizer=tf.keras.regularizers.l2(l2)))
    model = tf.keras.Sequential(layers)
    

    train_data = packed_train_data.batch(512).cache().prefetch(20)#.shuffle(500)
    test_data = packed_test_data.batch(1024).cache().prefetch(20)

    early_stoping = tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=10, restore_best_weights=True)

    #for shrink in range(10):
    model.compile(#optimizer='adam',
                optimizer=tf.keras.optimizers.Adam(),
                loss='sparse_categorical_crossentropy',
                metrics=['accuracy'])
    model.fit(train_data, epochs=EPOCHES,validation_data=test_data, callbacks=[early_stoping], class_weight=class_weights, use_multiprocessing=True, workers=10)
    #model.fit(train_data, epochs=EPOCHES,validation_data=test_data, class_weight=class_weights, use_multiprocessing=True, workers=10, verbose=2)
              
                                                               
    train_set = packed_train_data.batch(128).prefetch(20)
    test_set = packed_test_data.batch(1024).prefetch(20)

    train_prediction = model.predict(train_set)
    test_prediction = model.predict(test_set)
    train_matrix = create_confusion_matrix(train_prediction, train_labels)
    test_matrix = create_confusion_matrix(test_prediction, test_labels)
    
    print_matrix("train_matrix", train_matrix)
    print_matrix("test_matrix", test_matrix)
    return model

def predict_real_cell_traces(model):
    #print("Packing Dataset")
    #packed_real_dataset = real_dataset.map(PackNumericFeaturesNoLabel(numeric_features))
    
    count = [0.0,0.0,0.0]
    for part in range(0, 200):
        to_load = real_cell_trace_dir + "\\ProcessedTrace-part-"+("{:05d}".format(part))+".csv"
        print("Processing: " + to_load)
        real_dataset = get_dataset(to_load, numeric_features, 1, None, 1024).map(PackNumericFeaturesNoLabel(numeric_features))
        real_prediction = model.predict(real_dataset.batch(1024).prefetch(20), use_multiprocessing = True, workers=10)
        for pred in real_prediction:
            count[0] += pred[0]
            count[1] += pred[1]
            count[2] += pred[2]
        print("Predictions so far: ", end=' ')
        sum_of_count = 0
        for c in count:
            sum_of_count += c
        for c in count:
            print("%.0f%%" % (100 * (c / sum_of_count)), end=' ')
        print()
        with open("RealTraceResults-"+str(part)+".csv", 'w') as writer:
            writer.write("Auto,Transit,Active\n")
            for pred in real_prediction:
                writer.write(str(pred[0]))
                writer.write(',')
                writer.write(str(pred[1]))
                writer.write(',')
                writer.write(str(pred[2]))
                writer.write('\n')
    return


def get_value_int(prompt):
    while True:
        try:
            return int(input(prompt))
        except:
            print("Invalid integer")
            pass

def get_value_float(prompt):
    while True:
        try:
            return float(input(prompt))
        except:
            print("Invalid number")
            pass

while True:
    number_of_layers = get_value_int("Number of layers: ")
    layer_size = get_value_int("Number Of Nodes in Layer: ")
    dropout = get_value_float("Dropout Rate: ")
    l2 = get_value_float("L2: ")
    #learning_rate = get_value_float("Learning Rate: ")
    print("Training the model.")
    trained_model = train_model()
    if input("Predict real traces (y/[n]): ") == 'y':
        print("Predicting the mode of the real traces")
        predict_real_cell_traces(trained_model)
    print("Complete")
