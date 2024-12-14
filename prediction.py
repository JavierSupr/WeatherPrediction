import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from sklearn.preprocessing import MinMaxScaler
from tensorflow.keras.models import Sequential
from tensorflow.keras.layers import LSTM, Bidirectional, Dense, Dropout, BatchNormalization

data = pd.read_csv('data_filled.csv')  
data = data[['Tavg']]  
scaler = MinMaxScaler(feature_range=(0, 1))
data_scaled = scaler.fit_transform(data)

def create_dataset(data, look_back=30):
    X, y = [], []
    for i in range(len(data) - look_back):
        X.append(data[i:i + look_back, 0])
        y.append(data[i + look_back, 0])
    return np.array(X), np.array(y)

look_back = 30
X, y = create_dataset(data_scaled, look_back)
X = np.reshape(X, (X.shape[0], X.shape[1], 1))

train_size = int(len(X) * 0.8)
X_train, X_test = X[:train_size], X[train_size:]
y_train, y_test = y[:train_size], y[train_size:]

model = Sequential([
        Bidirectional(LSTM(100, return_sequences=True, 
                           input_shape=(look_back, 1), 
                           dropout=0.2, 
                           recurrent_dropout=0.2)),
        BatchNormalization(),
        
        Bidirectional(LSTM(75, return_sequences=True, 
                           dropout=0.2, 
                           recurrent_dropout=0.2)),
        BatchNormalization(),
        
        # Third LSTM layer
        Bidirectional(LSTM(50)),
        Dropout(0.3),
        
        # Dense layers for output
        Dense(50, activation='relu'),
        Dropout(0.2),
    Dense(1)
])

model.compile(optimizer='adam', loss='mean_squared_error')
model.fit(X_train, y_train, epochs=100, batch_size=32, validation_data=(X_test, y_test))

# Make predictions
train_predict = model.predict(X_train)
test_predict = model.predict(X_test)

# Invert predictions and scale back
train_predict = scaler.inverse_transform(train_predict)
test_predict = scaler.inverse_transform(test_predict)
y_train = scaler.inverse_transform([y_train])
y_test = scaler.inverse_transform([y_test])

# Extend data for future predictions
future_input = data_scaled[-look_back:]
future_input = future_input.reshape(1, look_back, 1)

future_predictions = []
for _ in range(30):  # Predict the next 30 days
    pred = model.predict(future_input)
    future_predictions.append(pred[0, 0])
    future_input = np.append(future_input[:, 1:, :], [[pred[0]]], axis=1)

future_predictions = scaler.inverse_transform(np.array(future_predictions).reshape(-1, 1))

# Plot results
plt.figure(figsize=(12, 6))
plt.plot(data.index[:len(train_predict)], train_predict, label='Train Predictions')
plt.plot(data.index[len(train_predict):len(train_predict) + len(test_predict)], test_predict, label='Test Predictions')
plt.plot(data.index, scaler.inverse_transform(data_scaled), label='Original Data')
plt.plot(range(len(data), len(data) + 30), future_predictions, label='Future Predictions')
plt.legend()
plt.title('Weather Prediction with Bi-LSTM')
plt.xlabel('Time')
plt.ylabel('Temperature')  # Replace with your target variable
plt.show()