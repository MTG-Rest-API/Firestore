from firebase_admin import initialize_app, firestore
from flask import Request, Response
from datetime import datetime

initialize_app(options={"projectId": "mtg-rest"})
# # measures
# # args: 
# #   measure (average, median, mean), maybe "stat"
# #   start (date,yyyymmdd), default 2023-11-03
# #   end (date, yyyymmdd), default today
# #   type (coverted mana cost, artist, color, primary card type, typed card) (maybe multiple filters?)
# # use different functions for different measures (one for sum, another for median etc)

# # price trends
# #  start date, default 2023-11-03
# #  end date, default 2023-11-03
# #  card name
# # return: List of all versions with name, id, version, set, price, artist, image

# # mean=color, cmc=
YYYY_MM_DD = "%Y%m%d"
def get_mean(start = "20231103", end = datetime.now().strftime(YYYY_MM_DD), of="color", cmc: str | int = -1, card_type: str ="all"):

#     # Parse Arguments
#     # Filter Cards
#     # Calculate Measure of Value (pandas?)

#     # Date is correct format, date is within valid range; Consider utils code

#     # Validate that of is valid of value (color, type, cmc, artist)

#     # Filter (This is probably shared between all)
#     # after all other args, perform a loop-switch statement to build a filter
#         # validate filter key is valid (easy switch default if not)
#         # validate filter value is valid (switch again)
#         # apply filter

#     # args
#     # of=value (color/cmc/basic type (creature, land, enchantment, artifact, sorcery, instant))
#     # startdate (released)
#     # enddate (released)
    db = firestore.client()
    #_, run = db.collection("run").add({"start": firestore.SERVER_TIMESTAMP})
    cards = db.collection("cards")

#     let start = new Date('2017-01-01');
# let end = new Date('2018-01-01');

# this.afs.collection('invoices', ref => ref
#   .where('dueDate', '>', start)
#   .where('dueDate', '<', end)
# );

    start = datetime.fromisoformat("2023-01-01") #, YYYY_MM_DD)
    # cards = cards.where("released", "==", 6)
    # cards = cards.where("red", "==", True).where("cmc == 6")
    cards = cards.where("released", ">", start)
    docs = cards.stream()
    i = 0
    for doc in docs:
        i += 1
        if i < 2:
            print(doc)
    print(i)
    # 1233

get_mean()