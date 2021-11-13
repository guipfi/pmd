# pyright: reportMissingImports=false

import pandas as pd

df = pd.read_csv("marcas_produtos.csv")
print((df["marca"] == 'unknown').sum())