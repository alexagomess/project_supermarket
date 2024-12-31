import pandas as pd


df = pd.read_excel("scripts/raw/de_para_produtos.xlsx")

df.reset_index(drop=True, inplace=True)
