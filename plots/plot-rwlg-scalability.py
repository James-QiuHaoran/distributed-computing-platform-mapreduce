#!/usr/bin/env python

import matplotlib.pyplot as plt
import numpy as np

x = np.array(['2', '4', '6', '8'])
y1 = np.array([560.8, 335.2, 201.6, 125.5])
e1 = np.array([6.2, 4.7, 7.1, 2.9])
y2 = np.array([32.5, 28.3, 20.5, 19.4])
e2 = np.array([2.8, 3.0, 1.0, 2.1])

fig = plt.figure()
ax = fig.add_subplot(111)
ax.set_xlabel('Number of Workers', fontsize = 14)
ax.set_ylabel('Job Completion Time', fontsize = 14)

linestyle = {"linestyle":"-", "linewidth":2, "markeredgewidth":2, "elinewidth":1, "capsize":3, "marker":'^'}
ax.errorbar(x, y1, yerr = e1, color="r", label='Map', **linestyle)
ax.errorbar(x, y2, yerr = e2, color="b", label='Reduce', **linestyle)
ax.legend()

plt.title('(4) Reverse Web-Link Graph Using Different # of Workers')

plt.show()
