import pickle
file = open('iclink.pkl', 'rb')
data = pickle.load(file)
print(data.shape)
print(data.head())
