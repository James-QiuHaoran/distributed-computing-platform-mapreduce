#!/usr/bin/env python

import matplotlib.pyplot as plt
import numpy as np

x = np.array(['50M', '100M', '150M', '200M'])
y1 = np.array([82.9, 93.4, 105.2, 125.4])
e1 = np.array([5.9, 4.6, 2.8, 3.5])
y2 = np.array([77.4, 109.6, 124.2, 155.4])
e2 = np.array([2.1, 4.1, 1.4, 5.8])

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
plt.title('(1) Word Count Comparison Between Hadoop and MapleJuice')

plt.show()
