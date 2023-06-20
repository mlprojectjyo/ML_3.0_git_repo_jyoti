from pymongo.mongo_client import MongoClient
import os
from pprint import pprint

import bson
from dotenv import load_dotenv


#uri = "mongodb://localhost:27017/?retryWrites=true&w=majority"
uri = "https://protect-de.mimecast.com/s/8Co2CDqXDoSjAoVoxsWAeHq" mongo uri
# Create a new client and connect to the server
client = MongoClient(uri)
# Send a ping to confirm a successful connection
try:
    client.admin.command('ping')
    print("Pinged your deployment. You successfully connected to MongoDB!")
except Exception as e:
    print(e)

mydb = client["mongo db name"]
mycol = mydb[" collection name"]
# load_dotenv(verbose=True)

total_count = mycol.count_documents({})

print("Total number of documents :",total_count)

#1
CarsDefault_qry = [
  {
    "$group": {
      "_id": "$sold",
      "count": { "$sum": 1 }
    }
  },
  {
    "$project": {
      "_id": 0,
      "Responded": "$_id",
      "count": 1,
      "percentage": {
        "$concat": [
          { "$toString": { "$multiply": [ { "$divide": [ "$count", total_count ] }, 100 ] } },
          "%"
        ]
      }
    }
  }
]

results2 = mycol.aggregate(CarsDefault_qry)

print('Cars Sold Analysis')
print('CarsSold\tCount\tPercentage')
for cust1 in results2:
    print("{}\t{}\t{}".format(cust1['Responded'],cust1['count'],cust1['percentage']))


print("\n\n")



# 2 get the owner types
OwnerDefault_qry = [
  {
    "$group": {
      "_id": "$owner",
      "count": { "$sum": 1 }
    }
  },
  {
    "$project": {
      "_id": 0,
      "Responded": "$_id",
      "count": 1,
      "percentage": {
        "$concat": [
          { "$toString": { "$multiply": [ { "$divide": [ "$count", total_count ] }, 100 ] } },
          "%"
        ]
      }
    }
  }
]

results2 = mycol.aggregate(OwnerDefault_qry)

print('Owner types')
print('Owner\tCount\tPercentage')
for cust1 in results2:
    print("{}\t{}\t{}".format(cust1['Responded'],cust1['count'],cust1['percentage']))

#
print("\n\n")
# 3 Wrt owner types
Owner_qry = [
  {
    "$group": {
      "_id": "$owner",
      "count": { "$sum": { "$cond": [{ "$eq": ["$sold", 'y'] }, 1, 0] } }
    }
  },
  {
    "$match": {
      "count": { "$gt": 0 }
    }
  }
]
results = mycol.aggregate(Owner_qry)

print("owner\tcount")
for cust in results:
     print("{}\t{}".format(cust["_id"],cust["count"]))


print("\n\n")
# 4 fuel vs no of cars sold
fuel_qry = [
  {
    "$group": {
      "_id": {
        "car1": "$fuel",
        "car": "$sold"
      },
      "total": {
        "$sum": 1
      }
    }
  },
  {
    "$group": {
      "_id": "$_id.car1",
      "car1": {
        "$push": "$$ROOT"
      },
      "total": {
        "$sum": "$total"
      }
    }
  },
  {
    "$addFields": {
      "car1": {
        "$map": {
          "input": "$car1",
          "in": {
            "_id": "$$this._id",
            "count":"$$this.total",
            "percentage": {
              "$multiply": [
                {
                  "$divide": [ "$$this.total", total_count ]
                },
                100
              ]
            }
          }
        }
      }
    }
  },
  {
    "$unwind": "$car1"
  },
  {
    "$replaceRoot": {
      "newRoot": "$car1"
    }
  }
]

results = mycol.aggregate(fuel_qry)

# print('fuel wrt cars sold')
# for cust in results:
#     print(cust)
#     # print(cust['percentage'])

print("fuel\t|carssold(Y/N)\t|Count\t|Percentage")

for cust in results:
    print("{}\t|{}\t|{}\t|{}".format(cust['_id']['car1'], cust['_id']['car'], cust['count'], cust['percentage']))



print("\n\n")
# 5 find out how many cars sodl vs owner type
owner_qry = [
  {
    "$group": {
      "_id": {
        "car1": "$owner",
        "car": "$sold "
      },
      "total": {
        "$sum": 1
      }
    }
  },
  {
    "$group": {
      "_id": "$_id.car1",
      "car1": {
        "$push": "$$ROOT"
      },
      "total": {
        "$sum": "$total"
      }
    }
  },
  {
    "$addFields": {
      "car1": {
        "$map": {
          "input": "$car1",
          "in": {
            "_id": "$$this._id",
            "count":"$$this.total",
            "percentage": {
              "$multiply": [
                {
                  "$divide": [ "$$this.total", total_count ]
                },
                100
              ]
            }
          }
        }
      }
    }
  },
  {
    "$unwind": "$car1"
  },
  {
    "$replaceRoot": {
      "newRoot": "$car1"
    }
  }
]

results = mycol.aggregate(owner_qry)

print("owner\t|carssold(Y/N)\t|Count\t|Percentage")

for cust in results:
    print("{}\t|{}\t|{}\t|{}".format(cust['_id']['car1'], cust['_id']['car'], cust['count'], cust['percentage']))

