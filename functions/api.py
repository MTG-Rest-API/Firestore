from firebase_admin import initialize_app, firestore
from flask import Request, Response
from datetime import datetime
from google.cloud.firestore_v1 import aggregation
from google.cloud.firestore_v1.base_query import FieldFilter, And, Or

initialize_app(options={"projectId": "mtg-rest"})

# # price trends
# #  start date, default 2023-11-03
# #  end date, default 2023-11-03
# #  card name
# # return: List of all versions with name, id, version, set, price, artist, image

MTG_RELEASED = "1993-08-05"
# We have to create an index for each value (color/basic types/artists)
# Does that mean we need to change the filters?
def get_mean(start: str = MTG_RELEASED, end: str = datetime.today().isoformat(), 
             of="color", min_cmc: int = 0, max_cmc: int = 99, card_type: str | None = None):

    # Initialize Client
    db = firestore.client()
    cards = db.collection("cards")

    # Parse Dates
    start: datetime = datetime.fromisoformat(start)
    end: datetime = datetime.fromisoformat(end)


    # filter_1 = FieldFilter("birthYear", "==", 1906)
    # filter_2 = FieldFilter("birthYear", "==", 1912)

    # # Create the union filter of the two filters (queries)
    # or_filter = Or(filters=[filter_1, filter_2])

    # # Execute the query
    # docs = col_ref.where(filter=or_filter).stream()


    a1 = ( cards
                .where(filter=FieldFilter("released", ">", start))
                .where(filter=FieldFilter("released", "<", end))
                .where(filter=FieldFilter("red", "==", True))
                .where(filter=FieldFilter("blue", "==", True))
                .where(filter=FieldFilter("green", "==", True))
                .where(filter=FieldFilter("white", "==", True))
                .where(filter=FieldFilter("colorless", "==", True))
                .where(filter=FieldFilter("black", "==", True))
            #    .where(filter=FieldFilter("cmc", ">", -1))
                # .where(filter=FieldFilter("cmc", "<", 99))
            )
    
    counter = aggregation.AggregationQuery(a1).count()
    for result in counter.get():
        print(result[0].value)

    # q2 = (    cards
    #                 .where(filter=FieldFilter("blue", "==", True))
    #                 .where(filter=FieldFilter("green", "==", True)))
    
    # redCounter = aggregation.AggregationQuery(a1).count()
    # blueCounter = aggregation.AggregationQuery(q2).count()
    # for result in redCounter.get():
    #     print(result[0].value)
    # for result in blueCounter.get():
    #     print(result[0].value)
    # cards = cards.where("released" "<", end)

    # red = cards.where(filter=FieldFilter("red", "==", True))
    # i = 0
    # for c in red.stream():
    #     i += 1
    # print(i)
    #print(red.stream().count())
   # blue = cards.where(filter=FieldFilter("blue", "==", True))

#     redCount = aggregation.AggregationQuery(red).count(alias="redCards")
#    # blueCount = aggregation.AggregationQuery(blue).count(alias="blue")

#     for result in redCount.get():
#         print(redCount[0].value)

    # for result in blueCount.get():
    #     print(blueCount[0].value)

    # agg = aggregation.AggregationQuery(cards)
    # agg.count(alias="all")
    # for result in agg.get():
    #     print(result[0].value)
    # # print(agg.count().get()[0].value)

#  aggregate_query.count(alias="all")

#     results = aggregate_query.get()
#     for result in results:
#         print(f"Alias of results from query: {result[0].alias}")
#         print(f"Number of results from query: {result[0].value}")

    # Parse arguments
    # (We're just assuming they're correct for now)
    # if cmc is not None:
    #     cards = cards.where("cmc", "==", cmc)

    # # TODO: Convert type-line to array, we can probably replace("- ", "") and then split on (" ")
    # if card_type != 'all':
    #     cards = cards.where("type_line")
    
    # match of:
    #     case "color":
    #         pass
    
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

    # start = datetime.fromisoformat("2023-01-01") #, YYYY_MM_DD)
    # # cards = cards.where("released", "==", 6)
    # # cards = cards.where("red", "==", True).where("cmc == 6")
    # cards = cards.where("released", ">", start)
    # docs = cards.stream()
    # i = 0
    # for doc in docs:
    #     i += 1
    #     if i < 2:
    #         print(doc)
    # print(i)
    # 1233

get_mean(start="2023-01-01")