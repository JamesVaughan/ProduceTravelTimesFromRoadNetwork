# This project is designed to estimate the Artificial Cellphone Traces
import tensorflow as tf
from tensorflow import keras
import functools
import os


EPOCHES = 20

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

LABEL_COLUMN = 'Result'
LABELS = [0, 1, 2]


raw_train_data = tf.data.experimental.make_csv_dataset(r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\ProduceTravelTimesFromRoadNetwork\bin\Release\netcoreapp2.2\SyntheticCellTraces.csv", 25
                                                       ,columns, label_name="Result", num_epochs= EPOCHES, ignore_errors = True)

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

preprocessing_layer = tf.keras.layers.DenseFeatures(numerical_columns)

# We need at least two layers in order to work with XOR
model = tf.keras.models.Sequential([
  #tf.keras.layers.Flatten(input_shape=(28, 28)),
  preprocessing_layer,
  tf.keras.layers.Dense(128, activation='relu'),
  tf.keras.layers.Dropout(0.2),
  tf.keras.layers.Dense(128, activation='relu'),
  tf.keras.layers.Dropout(0.2),
  tf.keras.layers.Dense(3, activation='softmax')
])

model.compile(optimizer='adam',
              loss='sparse_categorical_crossentropy', # Use this if you have more than one category
              # loss='binary_crossentropy', # Use this if the answers are just true / false
              metrics=['accuracy'])

train_data = raw_train_data.shuffle(500)

model.fit(train_data, epochs=EPOCHES)

model.save("ModeChoiceModel", True)

raw_test_data = tf.data.experimental.make_csv_dataset(r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\ProduceTravelTimesFromRoadNetwork\bin\Release\netcoreapp2.2\SyntheticCellTracesTest.csv", 5,
                                                     columns, field_delim=',', label_name="Result", num_epochs= EPOCHES, ignore_errors = True)
test_data = raw_test_data
model.evaluate(test_data)

print(model.summary())

