## Write code that does the following:

- Add year, month, day, hour columns

- benchmarks for cell based
  - with hive UDAF
  - with combine by key
- benchmarks for tile based
  - with hive UDAF
  - using combine by key

Functions
- returns a DF with key + list of (row, col, cells)
- returns a DF with key + list of tiles
- joins on row, cell, column
- joins on spatial key
- calculate splits
