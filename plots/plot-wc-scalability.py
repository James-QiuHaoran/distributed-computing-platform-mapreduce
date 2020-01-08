#!/usr/bin/env python

import matplotlib.pyplot as plt
import numpy as np

x = np.array(['2', '4', '6', '8'])
y1 = np.array([306.6, 169.9, 125.8, 104.4])
e1 = np.array([5.2, 1.7, 7.1, 7.9])
y2 = np.array([12.5, 12.3, 8.5, 8.1])
e2 = np.array([2.2, 2.0, 1.0, 1.1])

fig = plt.figure()
ax = fig.add_subplot(111)
ax.set_xlabel('Number of Workers', fontsize = 14)
ax.set_ylabel('Job Completion Time', fontsize = 14)

linestyle = {"linestyle":"-", "linewidth":2, "markeredgewidth":2, "elinewidth":1, "capsize":3, "marker":'^'}
ax.errorbar(x, y1, yerr = e1, color="r", label='Map', **linestyle)
ax.errorbar(x, y2, yerr = e2, color="b", label='Reduce', **linestyle)
ax.legend()

plt.title('(3) Word Count Using Different # of Workers')

plt.show()
