import itertools
import keras
import matplotlib.pyplot as plt
%matplotlib inline
import numpy as np
import os
import tensorflow as tf
tf.random.set_seed(100)
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import LabelEncoder, StandardScaler
from tensorflow.keras.layers import Dense
from tensorflow.keras.models import Sequential
import openpyxl
from keras.utils import np_utils

from IPython.display import display
from numpy.random import seed
import pandas as pd
seed(888)
from sklearn.metrics import confusion_matrix


# Load the data from GCP
#cha_data_raw

# Create a copy
#cha_data = cha_data_raw.copy()

# Check the data
#cha_data.info()

# check for nulls
#cha_data.

# Drop NaNs if there are any
#cha_data = cha_data.dropna(how='any')
#cha_data.isna().sum()

# Transform the categorical data
lab_enc = LabelEncoder()
encoded_data = lab_enc.fit_transform(cha_data[''])
encoded_data

X = cha_data.iloc[:, 1:-1].values
y = encoded_data

# Creating an 80% train and 20% test split
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=8)

# Scaling the data
scaler = StandardScaler()
X_train = scaler.fit_transform(X_train)
X_test = scaler.fit_transform(X_test)

# Creating the validation set
validation_size = ?
X_val_set = X_train[:validation_size]
y_val_set = y_train[:validation_size]

# Creates numpy arrays of X_train and y_train - do I need this?
X_train = X_train[validation_size:]
y_train = y_train[validation_size:]

# Converts a class vector (int) to binary class matrix
y_train = np_utils.to_categorical(y_train)
y_test = np_utils.to_categorical(y_test)

# Creating the structure of the Neural Network
# We have n input features and x classes
# (n + x) /2

# Structuring an initialising the NN
nnetwork = Sequential()
#nnetwork.add(Dropout(0.2, seed=42, input_shape=(total_input,)))
nnetwork.add(Dense(input_shape=(n, ), units=32, activation='relu')) # Need to change N
#nnetwork.add(Dropout(0.1))
nnetwork.add(Dense(units=16, activation='relu', name='hidden_layer_1'))
nnetwork.add(Dense(units=8, activation='relu', name='hidden_layer_2'))
nnetwork.add(Dense(units=4, activation='softmax', name='output_layer'))

# Compiling the ANN
nnetwork.compile(loss='categorical_crossentropy', optimizer='adam', metrics=['accuracy']) #'sparse_catergorical_crossentropy', 'adamax'

nnetwork.summary()

num_epochs = 100
nnetwork1 = nnetwork.fit(X_train, y_train, batch_size=10, epochs=num_epochs, validation_data=(X_val_set, y_val_set))

nnetwork1.history.keys()

plt.plot(nnetwork1.history['loss'])
plt.title('Loss')
plt.show()

plt.plot(nnetwork1.history['accuracy'])
plt.title('Accuracy')
plt.show()

# Evaluating the algorithm
test_accuracy = nnetwork.evaluate(X_test, y_test)

prediction = nnetwork.predict(X_test)
prediction[0]

y_test[0]

print(cha_data[''][np.argmax(prediction[0])])

#Testing the model.
test_loss, test_accuracy = nnetwork.evaluate(X_test, y_test)
print(f'The test loss is {X_test: 0.5} and the test accuracy is {y_test: 0.1%}')



