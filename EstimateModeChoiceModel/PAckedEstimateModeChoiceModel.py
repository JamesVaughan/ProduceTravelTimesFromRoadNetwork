################### ESTIMATION PARAMETERS ##########################
EPOCHES = 300 # This is a maximum number, not the number required
BATCH_SIZE = 128
layers_to_test = [3]
width_to_test = [64]
dropout_to_test = [0.5] #[0.2, 0.3, 0.4]
l2_to_test = [0.001]
train_file_path = r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\ProduceTravelTimesFromRoadNetwork\bin\Release\netcoreapp2.2\CombinedTrain.csv"
test_file_path = r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\ProduceTravelTimesFromRoadNetwork\bin\Release\netcoreapp2.2\CombinedTest.csv"
################### \ESTIMATION PARAMETERS ##########################

import functools

import numpy as np
import tensorflow as tf
from tensorflow import keras
import functools
import os

def create_confusion_matrix(predictions, dataset):
    confusion_matrix = [[0,0,0],[0,0,0],[0,0,0]]
    pos = 0
    for _, label in dataset:
        for l in label:
            for i in range(0, 3):
                confusion_matrix[l][i] += predictions[pos][i]
            pos += 1
    return confusion_matrix

def print_matrix(name, matrix):
    print("Matrix: " + str(name))
    for row in matrix:
        for col in row:
            print(col, end=',')
        print()

tf.debugging.set_log_device_placement(False)
# Make numpy values easier to read.
np.set_printoptions(precision=3, suppress=True)

LABEL_COLUMN = 'Result'
LABELS = [0, 1, 2]

def get_dataset(file_path, epochs, **kwargs):
  dataset = tf.data.experimental.make_csv_dataset(
      file_path,
      batch_size=BATCH_SIZE,
      label_name=LABEL_COLUMN,
      na_value="?",
      num_epochs=epochs,
      num_rows_for_inference=100000,
      sloppy = False,
      shuffle= False,
      ignore_errors=False, 
      **kwargs)
  return dataset

def show_batch(dataset):
  for batch, label in dataset.take(1):
    for key, value in batch.items():
      print("{:20s}: {}".format(key,value.numpy()))

raw_train_data = get_dataset(train_file_path, 10)
raw_test_data = get_dataset(test_file_path, 1)


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

LABEL_COLUMN = 'Result'
columns = ['Result']
columns = columns + numeric_features
LABELS = [0, 1, 2]
class_weights = {
        0 : 1.0,
        1 : 0.108206,
        2 : 1.0
    }


# temp_dataset = get_dataset(train_file_path, 
#                            select_columns=columns,
#                            column_defaults = column_types)
                           

# show_batch(temp_dataset)

def pack(features, label):
  return tf.stack(list(features.values()), axis=-1), label

# packed_dataset = temp_dataset.map(pack)

# for features, labels in packed_dataset.take(1):
#   print(features.numpy())
#   print()
#   print(labels.numpy())

class PackNumericFeatures(object):
  def __init__(self, names):
    self.names = names

  def __call__(self, features, labels):
    numeric_freatures = [features.pop(name) for name in self.names]
    numeric_features = [tf.cast(feat, tf.float32) for feat in numeric_freatures]
    numeric_features = tf.stack(numeric_features, axis=-1)
    features['numeric'] = numeric_features

    return features, labels

def normalize_numeric_data(data, mean, std):
      # Centre the data
      return (data - mean) / std

packed_train_data = raw_train_data.map(
    PackNumericFeatures(numeric_features))

packed_test_data = raw_test_data.map(
    PackNumericFeatures(numeric_features))

# example_batch, labels_batch = next(iter(packed_train_data))

import pandas as pd
desc = pd.read_csv(train_file_path)[numeric_features].describe()
MEAN = np.array(desc.T['mean'])
STD = np.array(desc.T['std'])

# Update the STD if the  result is 0 to replace it with 1.
STD[STD == 0.0] = 1.0



normalizer = functools.partial(normalize_numeric_data, mean=MEAN, std=STD)

numeric_column = tf.feature_column.numeric_column('numeric', normalizer_fn=normalizer, shape=[len(numeric_features)])
numeric_columns = [numeric_column]


for dropout in dropout_to_test:                   
    for number_of_layers in layers_to_test:
        for layer_size in width_to_test:
            for l2 in l2_to_test:
                preprocessing_layer = tf.keras.layers.DenseFeatures(numeric_columns)
                layers = [ preprocessing_layer ]

                for i in range(number_of_layers):
                    layers.append(tf.keras.layers.Dense(layer_size, activation='relu',
                                                       kernel_regularizer=tf.keras.regularizers.l2(l2)
                                                       ))
                    layers.append(tf.keras.layers.Dropout(dropout))
                
                layers.append(keras.layers.Dense(3, activation='softmax', kernel_regularizer=tf.keras.regularizers.l2(l2)))
                #layers.append(keras.layers.Dense(3, activation='softmax'))
                
                
                # We need at least two layers in order to work with XOR
                model = tf.keras.models.Sequential(layers)
                
                model.compile(optimizer='adam',
                              #optimizer='sgd',
                              loss='sparse_categorical_crossentropy', # Use this if you have more than one category
                              # loss='binary_crossentropy', # Use this if the answers are just true / false
                              metrics=['accuracy'])
                
                train_data = packed_train_data#.shuffle(500)
                test_data = packed_test_data
                
                
                early_stoping = tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=5)
                dir_name = "Models/ModeChoiceModel_" + str(EPOCHES) + "_" + str(number_of_layers) + "_" + str(layer_size) + "_" + str(dropout) + "_" + str(l2)
                if not os.path.exists(dir_name):
                    os.makedirs(dir_name)
                model_checkpoint = tf.keras.callbacks.ModelCheckpoint(dir_name, monitor='val_loss', save_best_only=True)
                
                model.fit(train_data, epochs=EPOCHES,
                          validation_data=test_data, class_weight=class_weights, callbacks=[early_stoping])
                                                                           
                train_set = train_data.take(-1)
                test_set = test_data.take(-1)

                train_prediction = model.predict(train_set)
                test_prediction = model.predict(test_set)
                train_matrix = create_confusion_matrix(train_prediction, train_set)
                test_matrix = create_confusion_matrix(test_prediction, test_set)
                
                print_matrix("train_matrix", train_matrix)
                print_matrix("test_matrix", test_matrix)
