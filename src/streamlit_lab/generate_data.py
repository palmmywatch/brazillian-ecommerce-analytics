
"""
Generate Synthetic E-commerce Data (Olist-style structure)
This creates realistic Brazilian e-commerce data for portfolio demonstration.
"""

from datetime import datetime
import os
import random

import numpy as np
import pandas as pd


# ==================================================
# Set seed for reproducibility
# ==================================================

np.random.seed(42)
random.seed(42)

# ==================================================
# Configuration
# ==================================================

NUM_CUSTOMERS = 15000
NUM_SELLERS = 500
NUM_PRODUCTS = 3000
NUM_ORDERS = 50000
START_DATE = datetime(2022, 1, 1)
END_DATE = datetime(2024, 6, 30)







