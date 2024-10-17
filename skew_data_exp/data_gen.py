import numpy as np
import pandas as pd

for z in [1.5, 2, 2.5, 3, 3.5, 4]:
    for i in range(3):
        data = np.random.zipf(z, 60000000)
        df = pd.DataFrame({"x": data})
        df.to_csv(f"/mydata/skew_data/z_{z}_{i}.csv", index=False)
