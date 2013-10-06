from datetime import datetime
from operator import itemgetter
import re
import os
from zipfile import ZipFile
from os.path import expanduser
from lxml import etree

VALID_NOTIFICATIONS = re.compile('\}notification(OK|EF|ZK|PO)$')
VALID_PROTOCOLS = re.compile('\}protocol(OK1|EF3|ZK1|ZK5|PO1)$')

VALID_FILE_PREFIXI = ['organization', 'notification', 'protocol', 'contract']
ORDERS = tuple(enumerate(VALID_FILE_PREFIXI))

DIGITS = re.compile('[^\w]')
identity = lambda x: x


def strip(s):
    return s.strip()


def nullable(aggregate_func=itemgetter(0), null_value=None):
    return lambda lst: aggregate_func(lst) if lst else null_value


def get_value(xml, xpath, transform=strip, aggregate=itemgetter(0)):
    return aggregate([transform(x) for x in (xml.xpath(xpath,
                     namespaces={'t': 'http://zakupki.gov.ru/oos/types/1'},
                     smart_strings=False))])


def dt(date):
    return datetime.strptime(date.strip(), '%Y-%m-%dT%H:%M:%S')


def d(date):
    return datetime.strptime(date.strip(), '%Y-%m-%d')


def phone(s):
    return DIGITS.sub('', s)


def read_lot(xml):
    return {
        'max_price': get_value(xml, './t:customerRequirements/t:customerRequirement/t:maxPrice/text()', float, sum),
        'lot_name': get_value(xml, './t:subject/text()'),
        'ordinal_number': get_value(xml, './t:ordinalNumber/text()', int)}


def read_lots_from_notification(xml):
    get_xml_value = lambda *args: get_value(xml, *args)
    notification = {
        'notification_number': get_xml_value('./t:notificationNumber/text()'),
        'create_date': get_xml_value('./t:createDate/text()', dt),
        'publish_date': get_xml_value('./t:publishDate/text()', dt),
        'notification_name': get_xml_value('./t:orderName/text()'),
        'href': get_xml_value('./t:href/text()'),
        'registration_number': get_xml_value('./t:order/t:placer/t:regNum/text()', int),
        'final_price': None,
        'contract_sign_date': None,
        'execution_date': None}

    lots_xml = get_value(xml, './t:lots', transform=identity)
    lots = [read_lot(lot_xml) for lot_xml in lots_xml.iterchildren()]

    return [dict(lot.items() + notification.items()) for lot in lots]


def read_contract(xml):
    get_xml_value = lambda *args, **kwargs: get_value(xml, *args, **kwargs)
    notification_number = get_xml_value('./t:foundation/t:order/t:notificationNumber/text()',
                                        aggregate=nullable(null_value=""))
    if notification_number:
        lot_number = get_xml_value('./t:foundation/t:order/t:lotNumber/text()', int)
    else:
        notification_number = get_xml_value('./t:foundation/t:other/t:notificationNumber/text()',
                                            aggregate=nullable(null_value=""))
        lot_number = 1

    return {
        'notification_number': notification_number,
        'lot_number': lot_number,
        'sign_date': get_xml_value('./t:signDate/text()', d),
        'price': get_xml_value('./t:price/text()', float),
        'current_contract_stage': get_xml_value('./t:currentContractStage/text()'),
        'execution': "-".join([get_xml_value('./t:execution/t:year/text()'),
                               get_xml_value('./t:execution/t:month/text()')])
    }


def safe_concat(*nullable_strings):
    """if all values are None than returns None"""
    not_null_strings = [s for s in nullable_strings if s]
    if not_null_strings:
        return ' '.join(not_null_strings)
    return None


def read_suppliers_and_contacts_from_protocols(xml):
    """Returns tuple of [Supplier] and [Contact] and [lot participant]"""
    suppliers = []
    contacts = []
    lot_participants = []
    notification_number = get_value(xml, './t:notificationNumber/text()')
    for protocol_lot_xml in get_value(xml, './t:protocolLots', transform=identity, aggregate=nullable(null_value=[])):
        lot_number = get_value(protocol_lot_xml, './t:lotNumber/text()', int)
        for application_xml in get_value(protocol_lot_xml,
                                         './t:applications',
                                         transform=identity,
                                         aggregate=nullable(null_value=[])):
            for participant_xml in get_value(application_xml, './t:applicationParticipants', transform=identity):
                inn = get_value(participant_xml, './t:inn/text()', int, nullable(null_value=None))
                if not inn:
                    print 'WARNING: INN is null'
                    continue
                form = get_value(participant_xml, './t:organizationForm/text()', aggregate=nullable())
                name = get_value(participant_xml, './t:organizationName/text()', aggregate=nullable())
                supplier = {
                    'inn': inn,
                    'name': safe_concat(form, name)
                }
                suppliers.append(supplier)
                contact = {
                    'inn': inn,
                    'last_name': get_value(participant_xml, './t:contactInfo/t:lastName/text()', aggregate=nullable(null_value='')),
                    'first_name': get_value(participant_xml, './t:contactInfo/t:firstName/text()', aggregate=nullable(null_value='')),
                    'middle_name': get_value(participant_xml, './t:contactInfo/t:middleName/text()', aggregate=nullable(null_value='')),
                    'email': get_value(participant_xml, './t:contactInfo/t:contactEMail/text()', aggregate=nullable()),
                    'phone': get_value(participant_xml, './t:contactInfo/t:contactPhone/text()', phone,
                                       aggregate=nullable()),
                    'fax': get_value(participant_xml, './t:contactInfo/t:contactFax/text()', phone, aggregate=nullable()),
                }
                contacts.append(contact)
                lot_participant = {
                    'notification_number': notification_number,
                    'lot_number': lot_number,
                    'supplier_inn': inn
                }
                lot_participants.append(lot_participant)

    return suppliers, contacts, lot_participants


def read_customer(xml):
    get_xml_value = lambda *args: get_value(xml, *args)
    return {
        'registration_number': get_xml_value('./t:regNumber/text()', int),
        'inn': get_xml_value('./t:inn/text()', int),
        'okato': get_xml_value('./t:factualAddress/t:OKATO/text()', int),
        'name': get_xml_value('./t:fullName/text()')
    }


def parse_file(f):
    for event, xml in etree.iterparse(f, huge_tree=True):
        if VALID_NOTIFICATIONS.search(str(xml.tag)):
            try:
                for lot_dict in read_lots_from_notification(xml):
                    print lot_dict
            except Exception, ex:
                print ex, xml.tag
        elif str(xml.tag).endswith('}organization'):
            cust_dict = read_customer(xml)
            print cust_dict
        elif VALID_PROTOCOLS.search(str(xml.tag)):
            suppliers, contacts, supplier_to_lot = read_suppliers_and_contacts_from_protocols(xml)
            for s, c, sl in zip(suppliers, contacts, supplier_to_lot):
                print c, s, sl
        elif str(xml.tag).endswith('}contract'):
            contract_dict = read_contract(xml)
            if not contract_dict['notification_number']:
                continue
            print contract_dict


def process_file(f, filename):
    if all([filename.find(prefix) == -1 for prefix in VALID_FILE_PREFIXI]):
        return
    if filename.endswith('.xml'):
        print "Parsing file", filename
        parse_file(f)


def process_any_file(file):
    if file.endswith('.zip'):
        with ZipFile(file) as zip_file:
            for file_under_zip in zip_file.namelist():
                with zip_file.open(file_under_zip) as f:
                    process_file(f, file + '!' + file_under_zip)
    else:
        with open(file, 'r') as f:
            process_file(f, file)


def get_file_parse_order(file_name):
    fn = file_name.lower()
    for order, prefix in ORDERS:
        if fn.startswith(prefix):
            return order
    return len(ORDERS)


def os_flat_walk(path):
    for root, subfolders, files in os.walk(path):
        for f in files:
            filepath = os.path.join(root, f)
            yield (filepath, get_file_parse_order(f))


def process_all_files():
    path = os.path.join(expanduser('~'), 'zakupki.gov.ru')
    for filepath, order in sorted(os_flat_walk(path), key=itemgetter(1)):
        if filepath.find('201308') == -1:
            continue
        if filepath.find('237') != -1:
            continue
        if filepath.find('235') != -1:
            return
        process_any_file(filepath)


if __name__ == "__main__":
    process_all_files()
