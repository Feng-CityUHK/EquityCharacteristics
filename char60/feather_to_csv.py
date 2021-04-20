import pickle as pkl
import pyarrow.feather as feather
import pandas as pd

with open('/Users/eric/Downloads/chars_rank_60.feather', 'rb') as f:
    chars = feather.read_feather(f)

print(chars.columns.values)

chars['jdate'] = pd.to_datetime(chars['jdate'])
chars['year'] = chars['jdate'].dt.year
chars_1970s = chars[chars['year'] < 1980]
chars_1980s = chars[(chars['year'] >= 1980) & (chars['year'] < 1990)]
chars_1990s = chars[(chars['year'] >= 1990) & (chars['year'] < 2000)]
chars_2000s = chars[(chars['year'] >= 1990) & (chars['year'] < 2010)]
chars_2010s = chars[(chars['year'] >= 2000) & (chars['year'] < 2020)]

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