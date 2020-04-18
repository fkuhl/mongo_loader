# mongo_loader
Utility that takes JSON data from the old PeriMeleon and loads
it into MongoDB.
Thid assumes the denormalized scheme, in which only Households
are stored in Mongo. (Members and Addreses are embedded.)
Along the way it fixes up id's for Members and Households.

# depends on
pm_data_types

## Install
git clone git@github.com:fkuhl/mongo_loader

cd mongo_loader

python3 -m pip install -r requirements.txt

## Run loader
python3 ./mongo_loader.py [-dir dir] filename