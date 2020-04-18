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

# try:
#     sys.path.index("/Users/fkuhl/Documents/workspace/pm_http/pm_data_types")
# except ValueError:
#     sys.path.append("/Users/fkuhl/Documents/workspace/pm_http/pm_data_types")

from pm_data_types.member import Member, MemberStatus, Sex, MaritalStatus, Transaction, TransactionType, Service, ServiceType
from pm_data_types.address import Address
from pm_data_types.household import Household

logging.basicConfig(filename='server.log', level=logging.DEBUG)
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


def none_empty(s):
    """If string s is None or empty, return None."""
    return s if s else None


def index_addresses(importedAddresses):
    """Return dict of Addresses by imported index."""
    i = 0
    index = {}
    for a in importedAddresses:
        edited = copy.deepcopy(a)
        edited.address2 = none_empty(a.address2)
        edited.country = none_empty(a.country)
        edited.email = none_empty(a.email)
        edited.home_phone = none_empty(a.home_phone)
        index[a.id] = edited
        if i % 10 == 0:
            log.info(f"address {edited.address}, {edited.city}")
        i += 1
    return index


def index_members(members, addresses_by_imported_index, mansion_in_the_sky):
    """Create collection of Member structs indexed by member's imported index.
     - Precondition: Addresses have been indexed, i.e., index_addresses has been executed.
     - Postcondition: Member structures have any temp_addresses embedded. Household index is still the imported index, not the Mongo.
     - Returns index of members.
     """
    index = {}  # {id : Member }
    i = 0
    for m in members:
        e = copy.deepcopy(m)
        e.middle_name = none_empty(m.middle_name)
        e.previous_family_name = none_empty(m.previous_family_name)
        e.name_suffix = none_empty(m.name_suffix)
        e.title = none_empty(m.title)
        e.nickname = none_empty(m.nickname)
        e.place_of_birth = none_empty(m.place_of_birth)
        e.household = m.household if m.household else mansion_in_the_sky
        if m.temp_address:
            if m.temp_address in addresses_by_imported_index:
                # imported integer index replaced by Address. 2 Kings 5:18
                e.temp_address = addresses_by_imported_index[m.temp_address]
            else:
                e.temp_address = None
                log.error(f"temp addr ind not known: {m.temp_address}")
        e.spouse = none_empty(m.spouse)
        e.divorce = none_empty(m.divorce)
        e.father = none_empty(m.father)
        e.mother = none_empty(m.mother)
        e.email = none_empty(m.email)
        e.work_email = none_empty(m.work_email)
        e.mobile_phone = none_empty(m.mobile_phone)
        e.work_phone = none_empty(m.work_phone)
        e.education = none_empty(m.education)
        e.employer = none_empty(m.employer)
        e.baptism = none_empty(m.baptism)
        index[m.id] = e
        if i % 20 == 0:
            log.info(f"member {m.full_name}")
        i += 1
    return index


def index_households(households, addresses_by_imported_index, members_by_imported_index, mansion_in_the_sky):
    """
    Create collection of Household objects indexed by household's imported index. Household objs are ready to be added to Mongo.
     - Precondition: Members have been indexed, i.e., index_members has been executed.
     - Postcondition: Household objs created, with Members and Addresses embedded.
        Members have imported Household indexes, not in Mongo yet.
        mansion_in_the_sky has been appended to array of HouseholdDocuments.
    """
    i = 0
    household_docs = []  # Households
    for h in households:
        d = copy.deepcopy(h)
        try:
            d.head = members_by_imported_index[h.head]
        except (KeyError):
            log.error(
                f"household {h.id}: no member imported for head {h.head}")
            continue  # just don't transfer that one
        if h.spouse:
            try:
                d.spouse = members_by_imported_index[h.spouse]
            except (KeyError):
                log.error(
                    f"household {h.id}: no member imported for head {h.spouse}")
        others = []  # Members
        for oi in h.others:
            try:
                others.append(members_by_imported_index[oi])
            except (KeyError):
                log.error(
                    f"household {h.id}: no member imported for other {oi}")
        d.others = others
        if h.address:
            d.address = addresses_by_imported_index[h.address]
        household_docs.append(d)
        if i % 10 == 0:
            log.info(f"household {d.head.full_name}")
        i += 1
    for member in members_by_imported_index.values():
        if member.household == mansion_in_the_sky.id:
            mansion_in_the_sky.others.append(member)
            log.info(f"placing {member.full_name} in mansion_in_the_sky")
    household_docs.append(mansion_in_the_sky)
    return household_docs


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
        ready_to_insert = json.loads(jsonpickle.encode(h))
        mongo_id = collection.insert_one(ready_to_insert).inserted_id
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
    i = 0
    for h in households:
        try:
            head_mongo = mongo_id_by_input_id[h.head.household]
        except (KeyError):
            log.error(
                f"head of {h.head.full_name}, no Mongo id corresp to {h.head.household}")
            raise
        h.head.household = head_mongo
        if h.spouse:
            try:
                spouse_mongo = mongo_id_by_input_id[h.spouse.household]
            except (KeyError):
                log.error(
                    f"spouse of {h.spouse.full_name}, no Mongo id corresp to {h.spouse.household}")
                raise
            h.spouse.household = spouse_mongo
        for other in h.others:
            try:
                other_mongo = mongo_id_by_input_id[other.household]
            except (KeyError):
                log.error(
                    f"other {other.full_name}, no Mongo id corresp to {other.household}")
                raise
            other.household = other_mongo
        ready_to_insert = json.loads(jsonpickle.encode(h))
        if i == 0:
            log.info("ready to update")
            pprint.pprint(ready_to_insert)
        filter = {"_id": ObjectId(h.id)}
        result = collection.replace_one(filter, ready_to_insert)
        if i % 20 == 0:
            log.info(
                f"{h.head.full_name} matched: {result.matched_count} replaced: {result.modified_count}")
        i += 1


def load_em_up(filename):
    #filename = "/Users/fkuhl/Desktop/members-py.json"
    with open(filename) as f:
        pickled = f.read()

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
