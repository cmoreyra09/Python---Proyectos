import json
import pandas as pd
x = [
  {
    "id": "A001",
    "name": "Tom",
    "math": 60,
    "physics": 66,
    "chemistry": 61
  },
  {
    "id": "A002",
    "name": "James",
    "math": 89,
    "physics": 76,
    "chemistry": 51
  },
  {
    "id": "A003",
    "name": "Jenny",
    "math": 79,
    "physics": 90,
    "chemistry": 78
  }
]

print(json.dumps(x,indent=2, sort_keys=True))
dataframe = pd.DataFrame(x)
dataframe
