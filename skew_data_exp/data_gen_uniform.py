import numpy as np
import pandas as pd

data = np.random.uniform(1, 100, 180000000)
df = pd.DataFrame({"x": data})
df.to_csv("/mydata/skew_data/uniform.csv", index=False)
