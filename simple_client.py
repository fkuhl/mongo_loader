from bson import ObjectId
import pymongo
from pymongo import MongoClient
from pm_data_types.member import Member, MemberStatus, Sex, MaritalStatus, Transaction, TransactionType, Service, ServiceType
from pm_data_types.address import Address
from pm_data_types.household import Household


def main():
    """Simple Mongo client to illustrate uses of pm_data_types."""
    client = MongoClient(host="localhost", port=27017)
    db = client["PeriMeleon"]
    collection = db["households"]
    # Yeah, having to use the jsonpickle names is ugly. We can wrap this.
    criterion = {"_Household__spouse._Member__given_name": "Jennifer"}
    print("Heads of households whose spouse is named 'Jennifer':")
    for household_dict in collection.find(criterion):
        # What comes from Mongo is a mere dict. This instantiates an object.
        household = Household.make_household(household_dict)
        # But now, look how nice properties are.
        print(household.head.full_name)

    pastors = []
    for household_dict in collection.find():
        household = Household.make_household(household_dict)
        pastors_in_household = list(filter(
            lambda m: m.status == MemberStatus.PASTOR, household.members))
        pastors.extend(pastors_in_household)
    print("'members' designated as pastors:")
    for p in pastors:
        print(p.full_name)
    # Ask me about "Shepherd, Good" ;-)

    mem = Member()
    mem.family_name = "Hornswoggle"
    mem.given_name = "Horatio"
    new_house = Household()
    new_house.head = mem
    mongo_id = collection.insert_one(new_house.mongoize()).inserted_id
    # the MongoDB _id is a complicated structure. We just store a string representation.
    new_house.id = str(mongo_id)
    print(f"inserted {new_house.id}")
    retrieved_dict = collection.find_one(
        filter={"_id": ObjectId(new_house.id)})
    retrieved_house = Household.make_household(retrieved_dict)
    print(f"retrieved {retrieved_house.head.full_name}")


if __name__ == '__main__':
    main()
