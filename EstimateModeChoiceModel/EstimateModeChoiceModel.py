# This project is designed to estimate the Artificial Cellphone Traces
import tensorflow as tf
from tensorflow import keras
import functools
import os

EPOCHES = 5


MEANS = []

# Create Columns
columns = ['Result']

for i in range(288):
    name = 'Distance' + str(i)
    columns.append(name)
    MEANS.append((name, 0.5))
for i in range(288):
    name = 'Active' + str(i)
    columns.append(name)
    MEANS.append((name, 0.5))
for i in ['OriginPopulationDensity', 'OriginEmploymentDensity','OriginHouseholdDensity','DestinationPopulationDensity','DestinationEmploymentDensity','DestinationHouseholdDensity']:
    name = i
    columns.append(name)
    MEANS.append((name, 1000))

LABEL_COLUMN = 'Result'
LABELS = [0, 1, 2]


raw_train_data = tf.data.experimental.make_csv_dataset(r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\ProduceTravelTimesFromRoadNetwork\bin\Release\netcoreapp2.2\SyntheticCellTraces.csv",
                                                       25, columns, label_name=LABEL_COLUMN, num_epochs= EPOCHES, ignore_errors = True)

# Process Categories
categorical_columns = []

# Process Continuous

def process_continuous_data(mean, data):
  # Normalize data
  data = tf.cast(data, tf.float32) * 1.0/(2.0*mean)
  return data


numerical_columns = []

for i in range(len(MEANS)):
  num_col = tf.feature_column.numeric_column(MEANS[i][0], normalizer_fn=functools.partial(process_continuous_data, MEANS[i][1]))
  numerical_columns.append(num_col)

raw_test_data = tf.data.experimental.make_csv_dataset(r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\ProduceTravelTimesFromRoadNetwork\bin\Release\netcoreapp2.2\SyntheticCellTracesTest.csv",
                25, columns, field_delim=',', label_name=LABEL_COLUMN, num_epochs= EPOCHES, ignore_errors = True)


layers_to_test = [2, 3, 4, 5]
width_to_test = [32, 64, 128, 256]
dropout_to_test = [0.2]

if not os.path.exists("Models"):
    os.makedirs("Models")

with open("Models/MetaData.csv","w") as writer:
    writer.write("Epochs,LayerSize,Layers,Dropout,TrainAccuracy,TestAccuracy,TrainLoss,TestLoss\n")

    for dropout in dropout_to_test:                   
        for number_of_layers in layers_to_test:
            for layer_size in width_to_test:
    
    
                preprocessing_layer = tf.keras.layers.DenseFeatures(numerical_columns)
                layers = [ preprocessing_layer ]
    
                for i in range(number_of_layers):
                    layers.append(tf.keras.layers.Dense(layer_size, activation='relu'))
                    layers.append(tf.keras.layers.Dropout(0.2))
                
                layers.append(keras.layers.Dense(3, activation='softmax'))
                
                
                # We need at least two layers in order to work with XOR
                model = tf.keras.models.Sequential(layers)
                
                model.compile(optimizer='adam',
                              loss='sparse_categorical_crossentropy', # Use this if you have more than one category
                              # loss='binary_crossentropy', # Use this if the answers are just true / false
                              metrics=['accuracy'])
                
                train_data = raw_train_data.shuffle(500)
                
                model.fit(train_data, epochs=EPOCHES)
                dir_name = "Models/ModeChoiceModel_" + str(EPOCHES) + "_" + str(number_of_layers) + "_" + str(layer_size) + "_" + str(dropout)
                if not os.path.exists(dir_name):
                    os.makedirs(dir_name)
                model.save(dir_name, True)
                

                print(model.summary())
                
                print ("Evaluating on the test dataset")
                results_test_loss , results_test_acc = model.evaluate(raw_test_data)
                print()
                
                print ("Evaluating on the training dataset")
                results_train_loss , results_train_acc = model.evaluate(raw_train_data)
                print()

                writer.write(str(EPOCHES) + "," + str(layer_size) + "," + str(number_of_layers) + "," + str(dropout) + ","  +\
                             str(results_train_acc) + "," + str(results_test_acc) + "," + str(results_train_loss) + "," + str(results_test_loss) + "\n")


    
    

