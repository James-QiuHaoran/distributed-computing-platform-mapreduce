#!/usr/bin/env python

import matplotlib.pyplot as plt
import numpy as np

x = np.array(['50M', '100M', '150M', '200M'])
y1 = np.array([95.5, 113.8, 131.0, 189.8])
e1 = np.array([0.6, 0.9, 1.3, 1.7])
y2 = np.array([72.2, 85.6, 108.7, 132.6])
e2 = np.array([5.8, 2.1, 2.0, 1.7])

fig = plt.figure()
ax = fig.add_subplot(111)
ax.set_xlabel('Data Set Size', fontsize = 14)
ax.set_ylabel('Job Completion Time', fontsize = 14)

#ax.axis([0, 6, 0, 35])
linestyle = {"linestyle":"-", "linewidth":2, "markeredgewidth":2, "elinewidth":1, "capsize":3, "marker":'^'}
ax.errorbar(x, y1, yerr = e1, color="r", label='Hadoop', **linestyle)
ax.errorbar(x, y2, yerr = e2, color="b", label='MapleJuice', **linestyle)
ax.legend()

# plt.errorbar(x, y, e, linestyle='-', marker='^')
plt.title('(2) Reverse Web Link Graph Comparison Between Hadoop and MapleJuice')

plt.show()
