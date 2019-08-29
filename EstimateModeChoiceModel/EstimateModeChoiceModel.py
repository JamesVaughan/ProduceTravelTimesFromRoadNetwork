# This project is designed to estimate the Artificial Cellphone Traces
import tensorflow as tf
from tensorflow import keras
import functools
import os

#print("GPU Available: ", tf.test.is_gpu_available())
tf.debugging.set_log_device_placement(False)

################### ESTIMATION PARAMETERS ##########################
EPOCHES = 20
BATCH_SIZE = 1024
layers_to_test = [3]
width_to_test = [64]
dropout_to_test = [0.5] #[0.2, 0.3, 0.4]
l2_to_test = [0.002]
################### \ESTIMATION PARAMETERS ##########################

training_data_path = r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\ProduceTravelTimesFromRoadNetwork\bin\Release\netcoreapp2.2\SyntheticCellTraces.csv"
test_data_path = r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\ProduceTravelTimesFromRoadNetwork\bin\Release\netcoreapp2.2\SyntheticCellTracesTest.csv"

MEANS = []

# Create Columns
columns = ['Result']
column_types = [tf.int32]

for i in range(288):
    name = 'Distance' + str(i)
    columns.append(name)
    MEANS.append((name, 0.5))
    column_types.append(tf.float32)
for i in range(288):
    name = 'Active' + str(i)
    columns.append(name)
    MEANS.append((name, 1.0))
    column_types.append(tf.float32)
for i in ['OriginPopulationDensity', 'OriginEmploymentDensity','OriginHouseholdDensity','DestinationPopulationDensity','DestinationEmploymentDensity','DestinationHouseholdDensity']:
    name = i
    columns.append(name)
    MEANS.append((name, 20000)) # half of twice the max
    column_types.append(tf.float32)
columns.append('TripDistance')
MEANS.append(('TripDistance', 15641.22))
column_types.append(tf.float32)

LABEL_COLUMN = 'Result'
LABELS = [0, 1, 2]

class_weights = {
        0 : 1.0,
        1 : 1.408206,
        2 : 0.954226
    }
#with tf.device('/GPU:0'):
    
raw_train_data = tf.data.experimental.make_csv_dataset(training_data_path,
                                                       BATCH_SIZE, columns, field_delim=',', column_defaults = column_types,
                                                      label_name=LABEL_COLUMN, num_epochs= EPOCHES, num_rows_for_inference=100000, ignore_errors = False)

raw_test_data = tf.data.experimental.make_csv_dataset(test_data_path,
                                                       BATCH_SIZE, columns, field_delim=',', column_defaults = column_types,
                                                       label_name=LABEL_COLUMN, num_epochs= EPOCHES, num_rows_for_inference=100000, ignore_errors = False)
# Process Categories
categorical_columns = []

# Process Continuous

def process_continuous_data(mean, data):
  # Normalize data
  data = tf.cast(data, tf.float32) / mean
  return data


numerical_columns = []

for i in range(len(MEANS)):
  num_col = tf.feature_column.numeric_column(MEANS[i][0], normalizer_fn=functools.partial(process_continuous_data, MEANS[i][1]))
  numerical_columns.append(num_col)

if not os.path.exists("Models"):
    os.makedirs("Models")

for dropout in dropout_to_test:                   
    for number_of_layers in layers_to_test:
        for layer_size in width_to_test:
            for l2 in l2_to_test:
                model_name = str(EPOCHES) + "," + str(layer_size) + "," + str(number_of_layers) + "," + str(dropout) + "," + str(l2)
                print (model_name)

                preprocessing_layer = tf.keras.layers.DenseFeatures(numerical_columns)
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
                   
                early_stoping = tf.keras.callbacks.EarlyStopping(monitor='val_loss', patience=5)
                dir_name = "Models/ModeChoiceModel_" + str(EPOCHES) + "_" + str(number_of_layers) + "_" + str(layer_size) + "_" + str(dropout) + "_" + str(l2)
                if not os.path.exists(dir_name):
                    os.makedirs(dir_name)
                model_checkpoint = tf.keras.callbacks.ModelCheckpoint(dir_name, monitor='val_loss', save_best_only=True)
                
                model.fit(raw_train_data, epochs=EPOCHES,
                          validation_data=raw_test_data, class_weight=class_weights, callbacks=[early_stoping
                                                                                                ])
