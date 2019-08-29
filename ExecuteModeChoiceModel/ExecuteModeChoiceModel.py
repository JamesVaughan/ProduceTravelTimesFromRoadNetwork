# This project is designed to execute the Artificial or Real Cellphone Traces
import tensorflow as tf
from tensorflow import keras
import functools
import os

#### PARAMETERS ####

run_on_real_traces = False

model_path = r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\EstimateModeChoiceModel\Models\ModeChoiceModel_20_3_64_0.5"
real_trace_path = r""
training_trace_path = r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\ProduceTravelTimesFromRoadNetwork\bin\Release\netcoreapp2.2\SyntheticCellTraces.csv"
testing_trace_path = r"C:\Users\phibr\source\repos\ProduceTravelTimesFromRoadNetwork\ProduceTravelTimesFromRoadNetwork\bin\Release\netcoreapp2.2\SyntheticCellTracesTest.csv"

####################

columns = ['Result']
MEANS = []
EPOCHES = 20
BATCH_SIZE = 256

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

#data_set = tf.data.experimental.make_csv_dataset(testing_trace_path,
#                    BATCH_SIZE, columns, label_name=LABEL_COLUMN, ignore_errors = True)

model = tf.keras.models.load_model(model_path, compile = True)

print(dir(model))
print("#################################################")
print(model.summary())
print()
results_test_loss , results_test_acc = model.evaluate(raw_test_data)
print()

