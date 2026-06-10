"""
Configure PySpark to use the current venv's Python interpreter.

Must run before any SparkSession is created so that executor processes
(which are separate subprocesses even in local mode) can import the same
packages installed in the venv (pyarrow, vaderSentiment, etc.).
"""

import os
import sys

os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
