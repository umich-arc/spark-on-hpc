# Example Python Program with NumPy and SciPy
# Basic Numerical Integration: the Trapezoid Rule
# https://nbviewer.jupyter.org/github/ipython/ipython/blob/master/examples/IPython%20Kernel/Trapezoid%20Rule.ipynb

from pyspark import SparkContext

sc = SparkContext(appName = "NumericIntegration")

import numpy as np
from scipy.integrate import quad
from scipy.version import version

# Use NumPy to define a simple function and sample it between 0 and 10 at 200 points
def f(x):
    return (x-3)*(x-5)*(x-7)+85

x = np.linspace(0, 10, 200)
y = f(x)

# Use NumPy to choose a region to integrate over and take only a few points in that region
a, b = 1, 8 # the left and right boundaries
N = 5 # the number of points
xint = np.linspace(a, b, N)
yint = f(xint)

# Compute the integral both at high accuracy and with the trapezoid approximation
# Use SciPy to calculate the integral
integral, error = quad(f, a, b)
print("The integral is:", integral, "+/-", error)

# Use NumPy to calculate the area with the trapezoid approximation
integral_trapezoid = sum( (xint[1:] - xint[:-1]) * (yint[1:] + yint[:-1])
    ) / 2
print("The trapezoid approximation with", len(xint),
    "points is:", integral_trapezoid)

print("############################################################################")
print("Python Version {0}".format(sc.pythonVer))
print("NumPy Version {0}".format(np.__version__))
print("SciPy Version {0}".format(version))
print("NumPy and SciPy are working!")
print("############################################################################")
