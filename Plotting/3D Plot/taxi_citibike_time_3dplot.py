from mpl_toolkits.mplot3d import Axes3D
import matplotlib.pyplot as plt
import numpy as np

data = np.genfromtxt("D:\\Downloads\\Big Data\\aggregatedata01.csv", delimiter=',',
                     names=['lonS', 'latS', 'lonD', 'latD', 'CitiBike', 'Taxi'])
fig = plt.figure()
ax = fig.gca(projection='3d')
ax.plot_trisurf(data['lonS'], data['lonD'], data['CitiBike'], color='c', linewidth=0.2, alpha=0.25, antialiased=True)
ax.plot_trisurf(data['lonS'], data['lonD'], data['Taxi'], color='g', linewidth=0.2, alpha=0.25, antialiased=True)
plt.show()
