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


def index_addresses(importedAddresses):
    """Return dict of Address instances by imported index."""
    # This editing was a premature optimization that might as well be removed.
    # (I'll remove this stuff after these changes are merged with master.)
    # When the Swift JSON encoder wncounters a nil property, it omits the property
    # from the JSON-encoded string. So I thought, if I could convert the many empty
    # strings in the PM data to nils, the JSON encoding would be much shorter.
    # But the Python JSON encoder represents Nones as "null", which is probably
    # more correct, but doesn't shorten the encoded string. So this "optimization"
    # was premature. And we all know what Knuth said about those.
    #     edited = copy.deepcopy(a)
    #     edited.address2 = a.address2 or None
    #     edited.country = a.country or None
    #     edited.email = a.email or None
    #     edited.home_phone = a.home_phone or None
    #     index[a.id] = edited
    log.info(f"indexed {len(importedAddresses)} addresses")
    return {a.id: a for a in importedAddresses}


def validate_members(members, addresses_by_imported_index):
    """
    Ensure that any temp_address id's are known.
    In keeping with the script-like nature of this program, we just log errors.
    """
    # There is no functional form of this (that I could find) that is shorter, or clearer.
    for m in members:
        if m.temp_address and not m.temp_address in addresses_by_imported_index:
            log.error(
                f"member {m.head.full_name} ({m.id}) had unk temp address {m.temp_address}")


def index_members(members, addresses_by_imported_index, mansion_in_the_sky):
    """Create collection of Member instances indexed by member's imported index.
     - Precondition: Addresses have been indexed, i.e., index_addresses has been executed.
     - Postcondition: Member structures have any temp_addresses embedded. 
       Household index is still the imported index, not the Mongo.
     - Returns index of members by imported index.
     """
    log.info(f"indexed {len(members)} members")

    def fix_member(m):
        # imported integer index replaced by Address. 2 Kings 5:18
        m.temp_address == addresses_by_imported_index[
            m.temp_address] if m.temp_address in addresses_by_imported_index else None
        # Oops! good catch!
        m.household = m.household or mansion_in_the_sky.id
        return m
    return {m.id: m or None for m in map(fix_member, members)}


def validate_households(households, addresses_by_imported_index, members_by_imported_index):
    """Check integrity of member and address references."""
    for h in households:
        if not h.head:
            log.error(f"household {h.id} has no head.")
        else:
            if not h.head in members_by_imported_index:
                log.error(f"household {h.id} has unk head {h.head}")
        if h.spouse and not h.spouse in members_by_imported_index:
            log.error(f"household {h.id} has unk spouse {h.spouse}")
        for other in h.others:
            if other not in members_by_imported_index:
                log.error(f"household {h.id} has unk other {other}")
        if h.address and not h.address in addresses_by_imported_index:
            log.error(f"household {h.id} has unk addr {h.address}")


def index_households(households, addresses_by_imported_index, members_by_imported_index, mansion_in_the_sky):
    """
    Create list of Household objects indexed by household's imported index. 
       Household objs are ready to be added to Mongo.
     - Precondition: Members have been indexed, i.e., index_members has been executed.
     - Postcondition: Household objs created, with Members and Addresses embedded.
        Members have imported Household indexes, not in Mongo yet.
        mansion_in_the_sky has been appended to list of Households.
     - Returns list of Households
    """
    log.info(f"indexed {len(households)} households")

    def fix_household(h):
        if h.head is not None and h.head in members_by_imported_index:
            h.head = members_by_imported_index[h.head]
        if h.spouse in members_by_imported_index:
            h.spouse = members_by_imported_index[h.spouse]
        others_in_index = filter(
            lambda idx: idx in members_by_imported_index, h.others)
        h.others = list(
            map(lambda idx: members_by_imported_index[idx], others_in_index))
        if h.address in addresses_by_imported_index:
            h.address = addresses_by_imported_index[h.address]
        return h
    # The conditional at the end of the comprehension is needed because one household has null head. We should catch that in a validaiton step
    households_list = [h for h in list(
        map(fix_household, households)) if h.head]
    mansion_in_the_sky.others = [m for m in members_by_imported_index.values()
                                 if m.household == mansion_in_the_sky.id]
    households_list.append(mansion_in_the_sky)
    return households_list


def store(mongo_collection, households):
    """
    Store preliminary version of households in Mongo, creating an index
    from imported household id to MongDB id.
    Households are mutated to store Mongo id.
    """
    mongo_id_by_input_id = {}  # {input id : mongo id <as string>}
    try:
        mongo_collection.drop()
    except:
        log.error(f"drop failed, {sys.exc_info()[0]}")
        return
    i = 0
    for h in households:
        input_id = h.id
        mongo_id = mongo_collection.insert_one(h.mongoize()).inserted_id
        mongo_id_by_input_id[input_id] = str(mongo_id)
        # h passed by reference, so input Household is being mutated
        h.id = str(mongo_id)  # swap imported id for mongo. 2 Kings 5:18
        if i % 20 == 0:
            log.info(f"imported id {input_id} stored as {str(mongo_id)}")
        i += 1
    return mongo_id_by_input_id


def fixup_and_update(mongo_collection, households, mongo_id_by_input_id):
    """
    Fixup households: In each Member, replace the imported Household index
        with the Mongo id.
    - Precondition: mongo_id_by_input_id is populated.
    - Postcondition: households are stored in final form.
    - Returns: no return; household list has been mutated
    """
    def fix_household(h):
        if h.head.household in mongo_id_by_input_id:
            h.head.household = mongo_id_by_input_id[h.head.household]
        if h.spouse and h.spouse.household in mongo_id_by_input_id:
            h.spouse.household = mongo_id_by_input_id[h.spouse.household]
        for m in h.others:
            m.household = mongo_id_by_input_id[m.household]
        return h

    i = 0
    for h in map(fix_household, households):
        ready_to_insert = h.mongoize()
        if i == 0:
            log.info("ready to update")
            pprint.pprint(ready_to_insert)
        criterion = {"_id": ObjectId(h.id)}
        result = mongo_collection.replace_one(criterion, ready_to_insert)
        if i % 20 == 0:
            log.info(
                f"{h.head.full_name} matched: {result.matched_count} replaced: {result.modified_count}")
        i += 1


def load_em_up(filename, dbhost):
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

    addresses_by_imported_index = index_addresses(addresses)

    mansion_in_the_sky = make_mansion_in_the_sky()
    validate_members(members, addresses_by_imported_index)
    members_by_imported_index = index_members(
        members, addresses_by_imported_index, mansion_in_the_sky)

    validate_households(
        households, addresses_by_imported_index, members_by_imported_index)
    households_ready_to_store = index_households(
        households, addresses_by_imported_index, members_by_imported_index, mansion_in_the_sky)

    client = MongoClient(host=dbhost, port=27017)
    db = client["PeriMeleon"]
    collection = db["households"]
    mongo_id_by_input_id = store(collection, households_ready_to_store)
    log.info(
        f"collection has {collection.estimated_document_count()} households")

    log.info(f"first household id: {households_ready_to_store[0].id}")
    log.info(f"hh: {households_ready_to_store[0]}")
    log.info(f"hh: {households_ready_to_store[1]}")
    log.info(f"hh: {households_ready_to_store[2]}")
    log.info(f"mem: {households_ready_to_store[0].head}")
    log.info(f"mem: {households_ready_to_store[1].head}")
    log.info(f"mem: {households_ready_to_store[2].head}")
    log.info(f"mem: {households_ready_to_store[3].head}")
    fixup_and_update(collection, households_ready_to_store,
                     mongo_id_by_input_id)
    log.info("And we're done.")


def parse_args(args=None):
    parser = ArgumentParser(
        description="Load data from elder PeriMeleon into MongoDB")
    parser.add_argument('-d', '--dir', default='.',
                        help="Directory containing data")
    parser.add_argument('-db', '--dbhost', default='localhost',
                        help="MongoDB server hostname")
    parser.add_argument("filename", type=str, help="data file")
    return parser.parse_args()


def main(args=None):
    args = parse_args(args)
    os.chdir(args.dir)
    load_em_up(args.filename, args.dbhost)


if __name__ == '__main__':
    main(sys.argv)
