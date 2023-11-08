from datetime import datetime
import pandas as pd
from firebase_admin import firestore

def transform(data):
    # FILTER CARD DATA
    print(datetime.now(), ": Normalizing Card Data")
    df = pd.json_normalize(data)

    print(datetime.now(), ": Filtering Card Data")

    # Simple Conversions
    df["released"] = pd.to_datetime(df["released_at"], errors='coerce').astype(str)
    df["cmc"] = pd.to_numeric(df["cmc"], errors='coerce').fillna(0)
    df["power"] = pd.to_numeric(df["power"], errors='coerce').fillna(0)
    df["toughness"] = pd.to_numeric(df["toughness"], errors='coerce').fillna(0)

    # Colors
    df["red"] = df["colors"].apply(lambda x: type(x) is list and "R" in x)
    df["blue"] = df["colors"].apply(lambda x: type(x) is list and  "U" in x)
    df["green"] = df["colors"].apply(lambda x: type(x) is list and "G" in x)
    df["white"] = df["colors"].apply(lambda x: type(x) is list and "W" in x)
    df["black"] = df["colors"].apply(lambda x: type(x) is list and "B" in x)
    df["colorless"] = df["colors"].apply(lambda x: type(x) is list and len(x) == 0)

    # Legalities
    df["standard"] = df["legalities.standard"] == "legal"
    df["modern"] = df["legalities.modern"] == "legal"
    df["vintage"] = df["legalities.vintage"] == "legal"
    df["legacy"] = df["legalities.legacy"] == "legal"

    # Prices
    df["usd"] = pd.to_numeric(df["prices.usd"], errors='coerce').fillna(0.0)
    df["usd_foil"] = pd.to_numeric(df["prices.usd_foil"], errors='coerce').fillna(0.0)
    df["eur"] = pd.to_numeric(df["prices.eur"], errors='coerce').fillna(0.0)
    df["eur_foil"] = pd.to_numeric(df["prices.eur_foil"], errors='coerce').fillna(0.0)

    # Dropping Double-Faced Cards
    filter = df.name.apply(lambda x: "//" not in x)
    df = df[filter]

    # Selecting only necessary columns
    df = pd.DataFrame(data=df, columns=["id", "name", "released", "uri", "mana_cost", "cmc", "type_line", "oracle_text",
                                            "power", "toughness", "red", "green", "blue", "white", "black", "colorless", "keywords",
                                            "standard", "modern", "vintage", "legacy", "reserved", "foil", "nonfoil",
                                            "promo", "reprint", "variation", "set_id", "rarity", "full_art",
                                            "usd", "usd_foil", "eur", "eur_foil"])


    print(datetime.now(), ": Writing Documents", flush=True)
    cards = df.to_dict(orient="records")
    year, month, day = str(datetime.now().date()).split("-")
    for card in cards:
        card["prices"] = {
            year: {
                month: {
                    day: {
                        "usd": card["usd"],
                        "eur": card["eur"],
                        "usd_foil": card["usd_foil"],
                        "eur_foil": card["eur_foil"]
                    }
                }
            }
        }

        del card["eur"]
        del card["usd"]
        del card["usd_foil"]
        del card["eur_foil"]
       # del card["released"]
    return cards