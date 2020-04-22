import sys
import os
import copy
import json
import jsonpickle
from datetime import date
from bson import ObjectId
import logging
import pymongo
from pymongo import MongoClient
import uuid
import pprint
from argparse import ArgumentParser

from pm_data_types.member import Member, MemberStatus, Sex, MaritalStatus, Transaction, TransactionType, Service, ServiceType
from pm_data_types.address import Address
from pm_data_types.household import Household

logging.basicConfig(filename='log/server.log', level=logging.DEBUG)
logging.getLogger().addHandler(logging.StreamHandler(sys.stdout))
logging.getLogger('asyncio').setLevel(logging.WARNING)
log = logging.getLogger('HandlerLogger')
pp = pprint.PrettyPrinter(indent=4)


def make_mansion_in_the_sky():
    """DEAD members must still belong to a household, to be included in the denormalized data.
    As DEAD members are imported they are added to mansionInTheSky.
    And of course each household must have a head.
    """
    mansion_in_the_sky_temp_id = str(uuid.uuid4())
    goodShepherd = Member()
    goodShepherd.family_name = "Shepherd"
    goodShepherd.given_name = "Good"
    goodShepherd.place_of_birth = "Bethlehem"
    goodShepherd.status = MemberStatus.PASTOR  # not counted against communicants
    goodShepherd.resident = False  # not counted against residents
    goodShepherd.ex_directory = True  # not included in directory
    goodShepherd.household = mansion_in_the_sky_temp_id

    mansion_in_the_sky = Household()
    mansion_in_the_sky.head = goodShepherd
    mansion_in_the_sky.id = mansion_in_the_sky_temp_id
    return mansion_in_the_sky

# Remove comment. Instead of wrapping an expression with this function,
# use a logical or. 
# so instead of: foo = none_empty(bar),
# use: foo = bar or None
# def none_empty(s):
#     """If string s is None or empty, return None."""
#     return s if s else None


def index_addresses(importedAddresses):
    """Return dict of Addresses by imported index."""
    # i = 0
    # index = {}
    # for a in importedAddresses:
    #     edited = copy.deepcopy(a)
    #     edited.address2 = a.address2 or None
    #     edited.country = a.country or None
    #     edited.email = a.email or None
    #     edited.home_phone = a.home_phone or None
    #     index[a.id] = edited
    #     if i % 10 == 0:
    #         log.info(f"address {edited.address}, {edited.city}")
    #     i += 1
    # return index
    #Remove comment: dict comprehensions are da bomb
    log.info(f"indexed {len(importedAddresses)} addresses")
    return {a.id: a or None for a in importedAddresses}


def index_members(members, addresses_by_imported_index, mansion_in_the_sky):
    """Create collection of Member structs indexed by member's imported index.
     - Precondition: Addresses have been indexed, i.e., index_addresses has been executed.
     - Postcondition: Member structures have any temp_addresses embedded. Household index is still the imported index, not the Mongo.
     - Returns index of members.
     """
    # index = {}  # {id : Member }
    # i = 0
    # for m in members:
    #     e = copy.deepcopy(m)
    #     e.middle_name = m.middle_name or None
    #     e.previous_family_name = m.previous_family_name or None
    #     e.name_suffix = m.name_suffix or None
    #     e.title = m.title or None
    #     e.nickname = m.nickname or None
    #     print(m.family_name)
    #     e.place_of_birth = m.place_of_birth or None
    #     e.household = m.household if m.household else mansion_in_the_sky # <== need the id here, not the object!
    #     if m.temp_address:
    #         if m.temp_address in addresses_by_imported_index:
    #             # imported integer index replaced by Address. 2 Kings 5:18
    #             e.temp_address = addresses_by_imported_index[m.temp_address]
    #         else:
    #             e.temp_address = None
    #             log.error(f"temp addr ind not known: {m.temp_address}")
    #     e.spouse = m.spouse or None
    #     e.divorce = m.divorce or None
    #     e.father = m.father or None
    #     e.mother = m.mother or None
    #     e.email = m.email or None
    #     e.work_email = m.work_email or None
    #     e.mobile_phone = m.mobile_phone or None
    #     e.work_phone = m.work_phone or None
    #     e.education = m.education or None
    #     e.employer = m.employer or None
    #     e.baptism = m.baptism or None
    #     index[m.id] = e
    #     if i % 20 == 0:
    #         log.info(f"member {m.full_name}")
    #     i += 1
    # return index
    # Remove comment: You can mutate values in a dict comprehensions also
    log.info(f"indexed {len(members)} members")
    def fix_member(m):
        m.temp_address == addresses_by_imported_index[m.temp_address] if m.temp_address in addresses_by_imported_index else None
        m.household = m.household or mansion_in_the_sky.id
        return m
    return {m.id: m or None for m in list(map(fix_member, members))}



def index_households(households, addresses_by_imported_index, members_by_imported_index, mansion_in_the_sky):
    """
    Create collection of Household objects indexed by household's imported index. Household objs are ready to be added to Mongo.
     - Precondition: Members have been indexed, i.e., index_members has been executed.
     - Postcondition: Household objs created, with Members and Addresses embedded.
        Members have imported Household indexes, not in Mongo yet.
        mansion_in_the_sky has been appended to array of HouseholdDocuments.
    """
    # i = 0
    # household_docs = []  # Households
    # for h in households:
    #     d = copy.deepcopy(h)
    #     try:
    #         d.head = members_by_imported_index[h.head]
    #     except (KeyError):
    #         log.error(
    #             f"household {h.id}: no member imported for head {h.head}")
    #         continue  # just don't transfer that one
    #     if h.spouse:
    #         try:
    #             d.spouse = members_by_imported_index[h.spouse]
    #         except (KeyError):
    #             log.error(
    #                 f"household {h.id}: no member imported for spouse {h.spouse}")
    #     others = []  # Members
    #     for oi in h.others:
    #         try:
    #             others.append(members_by_imported_index[oi])
    #         except (KeyError):
    #             log.error(
    #                 f"household {h.id}: no member imported for other {oi}")
    #     d.others = others
    #     if h.address:
    #         d.address = addresses_by_imported_index[h.address]
    #     household_docs.append(d)
    #     if i % 10 == 0:
    #         log.info(f"household {d.head.full_name}")
    #     i += 1
    # for member in members_by_imported_index.values():
    #     if member.household == mansion_in_the_sky.id:
    #         mansion_in_the_sky.others.append(member)
    #         log.info(f"placing {member.full_name} in mansion_in_the_sky")
    # household_docs.append(mansion_in_the_sky)
    # return household_docs
    log.info(f"indexed {len(households)} households")
    def fix_household(h):
        if h.head is not None and h.head in members_by_imported_index: h.head = members_by_imported_index[h.head]
        if not h.head:
            print('Household with no head\n', h)
        if h.spouse in members_by_imported_index: h.spouse =  members_by_imported_index[h.spouse]
        filtered_indexes = filter(lambda idx: idx in members_by_imported_index, h.others)
        h.others = list(map(lambda idx: members_by_imported_index[idx], filtered_indexes))
        if h.address in  addresses_by_imported_index:
            h.address = addresses_by_imported_index[h.address]
        return h
    #The conditional at the end of the comprehension is needed because one household has null head. We should catch that in a validaiton step
    households_list = [h for h in list(map(fix_household, households)) if h.head]
    #Remove comment: list comprehension with filter is an elegant way to set mansion members
    mansion_in_the_sky.others = [m for m in members_by_imported_index.values() if m.household == mansion_in_the_sky.id]
    households_list.append(mansion_in_the_sky)
    return households_list


def store(collection, households):
    """
    Store preliminary version of households in Mongo, creating an index
    from imported household id to MongDB id.
    Households are mutated to store Mongo id.
    """
    mongo_id_by_input_id = {}  # {input id : mongo id <as string>}
    try:
        collection.drop()
    except:
        log.error(f"drop failed, {sys.exc_info()[0]}")
        return
    i = 0
    for h in households:
        input_id = h.id
        mongo_id = collection.insert_one(h.mongoize()).inserted_id
        mongo_id_by_input_id[input_id] = str(mongo_id)
        # h passed by reference, so input Household is being mutated
        h.id = str(mongo_id)  # swap imported id for mongo. 2 Kings 5:18
        if i % 20 == 0:
            log.info(f"imported id {input_id} stored as {str(mongo_id)}")
        i += 1
    return mongo_id_by_input_id


def fixup_and_update(collection, households, mongo_id_by_input_id):
    """
    Fixup households: In each Member, replace the imported Household index
        with the Mongo id.
    - Precondition: mongo_id_by_input_id is populated.
    - Postcondition: households are stored in final form.
    - Returns: no return; household list has been mutated
    """
    # i = 0
    # for h in households:
    #     try:
    #         head_mongo = mongo_id_by_input_id[h.head.household]
    #     except (KeyError):
    #         log.error(
    #             f"head of {h.head.full_name}, no Mongo id corresp to {h.head.household}")
    #         raise
    #     h.head.household = head_mongo
    #     if h.spouse:
    #         try:
    #             spouse_mongo = mongo_id_by_input_id[h.spouse.household]
    #         except (KeyError):
    #             log.error(
    #                 f"spouse of {h.spouse.full_name}, no Mongo id corresp to {h.spouse.household}")
    #             raise
    #         h.spouse.household = spouse_mongo
    #     for other in h.others:
    #         try:
    #             other_mongo = mongo_id_by_input_id[other.household]
    #         except (KeyError):
    #             log.error(
    #                 f"other {other.full_name}, no Mongo id corresp to {other.household}")
    #             raise
    #         other.household = other_mongo
    #     ready_to_insert = h.mongoize()
    #     if i == 0:
    #         log.info("ready to update")
    #         pprint.pprint(ready_to_insert)
    #     criterion = {"_id": ObjectId(h.id)}
    #     result = collection.replace_one(criterion, ready_to_insert)
    #     if i % 20 == 0:
    #         log.info(
    #             f"{h.head.full_name} matched: {result.matched_count} replaced: {result.modified_count}")
    #     i += 1
    def fix_household(h):
        if h.head.household in mongo_id_by_input_id: h.head.household = mongo_id_by_input_id[h.head.household]
        if h.spouse and h.spouse.household in mongo_id_by_input_id: h.spouse.household = mongo_id_by_input_id[h.spouse.household]
        for m in h.others: m.household = mongo_id_by_input_id[m.household]
        return h

    i = 0
    for h in map(fix_household, households):
        ready_to_insert = h.mongoize()
        if i == 0:
            log.info("ready to update")
            pprint.pprint(ready_to_insert)
        criterion = {"_id": ObjectId(h.id)}
        result = collection.replace_one(criterion, ready_to_insert)
        if i % 20 == 0:
            log.info(
                f"{h.head.full_name} matched: {result.matched_count} replaced: {result.modified_count}")
        i+= 1


def load_em_up(filename):
    #filename = "/Users/fkuhl/Desktop/members-py.json"
    with open(filename) as f:
        pickled = f.read()

    # This works because PeriMeleon was carefully adjusted to emit jsonpickle-friendly stuff.
    unpickled = jsonpickle.decode(pickled)
    addresses = unpickled['addresses']
    households = unpickled['households']
    members = unpickled['members']
    log.info(
        f"addr: {len(addresses)} households: {len(households)} members: {len(members)}")
    log.info(
        f"p c: {addresses[0].postal_code} h: {households[0].spouse} m: {members[0].date_of_birth}")
    addresses_by_imported_index = index_addresses(addresses)
    mansion_in_the_sky = make_mansion_in_the_sky()
    members_by_imported_index = index_members(
        members, addresses_by_imported_index, mansion_in_the_sky)
    households_ready_to_store = index_households(
        households, addresses_by_imported_index, members_by_imported_index, mansion_in_the_sky)
    client = MongoClient(host="localhost", port=27017)
    db = client["PeriMeleon"]
    collection = db["households"]
    mongo_id_by_input_id = store(collection, households_ready_to_store)
    log.info(
        f"collection has {collection.estimated_document_count()} households")
    log.info(f"first household id: {households_ready_to_store[0].id}")
    fixup_and_update(collection, households_ready_to_store,
                     mongo_id_by_input_id)
    log.info("And we're done.")


def parse_args(args=None):
    parser = ArgumentParser(
        description="Load data from elder PeriMeleon into MongoDB")
    parser.add_argument('-d', '--dir', default='.',
                        help="Directory containing data")
    parser.add_argument("filename", type=str, help="data file")
    return parser.parse_args()


def main(args=None):
    args = parse_args(args)
    os.chdir(args.dir)
    load_em_up(args.filename)


if __name__ == '__main__':
    main(sys.argv)
