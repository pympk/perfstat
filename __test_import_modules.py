'''An example file that imports some of the installed modules'''
# https://blog.jayway.com/2019/12/28/pyenv-poetry-saviours-in-the-python-chaos/

import pandas as pd
import numpy as np
# import tensorflow as tfp
# import tensorflow_text as text
import matplotlib.pyplot as plt
import sys
from platform import python_version

print("python_path: ", sys.executable)
print("python_version: ",  python_version())

if __name__ == "__main__":
    # If the modules can't be imported, the following print won't happen
    print("Successfully imported the modules!")
