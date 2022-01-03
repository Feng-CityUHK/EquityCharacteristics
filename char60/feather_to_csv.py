import pickle as pkl
import pyarrow.feather as feather
import pandas as pd

# with open('chars60_raw_imputed.feather', 'rb') as f:
#     chars = feather.read_feather(f)

with open('chars60_rank_imputed.feather', 'rb') as f:
    chars = feather.read_feather(f)

print(chars.columns.values)

chars['date'] = pd.to_datetime(chars['date'])
chars['year'] = chars['date'].dt.year
chars_1970s = chars[chars['year'] < 1980]
chars_1980s = chars[(chars['year'] >= 1980) & (chars['year'] < 1990)]
chars_1990s = chars[(chars['year'] >= 1990) & (chars['year'] < 2000)]
chars_2000s = chars[(chars['year'] >= 2000) & (chars['year'] < 2010)]
chars_2010s = chars[(chars['year'] >= 2010) & (chars['year'] < 2020)]
chars_2020s = chars[(chars['year'] >= 2020) & (chars['year'] < 2030)]

# raw
# chars_1970s.to_csv('chars60_raw_1970s.csv', index=0)
# chars_1980s.to_csv('chars60_raw_1980s.csv', index=0)
# chars_1990s.to_csv('chars60_raw_1990s.csv', index=0)
# chars_2000s.to_csv('chars60_raw_2000s.csv', index=0)
# chars_2010s.to_csv('chars60_raw_2010s.csv', index=0)

# rank
chars_1970s.to_csv('chars60_rank_1970s.csv', index=0)
chars_1980s.to_csv('chars60_rank_1980s.csv', index=0)
chars_1990s.to_csv('chars60_rank_1990s.csv', index=0)
chars_2000s.to_csv('chars60_rank_2000s.csv', index=0)
chars_2010s.to_csv('chars60_rank_2010s.csv', index=0)
chars_2020s.to_csv('chars60_rank_2020s.csv', index=0)