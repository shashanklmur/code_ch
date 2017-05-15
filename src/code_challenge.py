import os.path
import json
import pytz

from collections import defaultdict
from datetime import datetime
from dateutil import parser


class Data:
    """
    Class to describe event data
    """

    # Customer Events Master data
    customer_master_dict = defaultdict(dict)

    # Orders Master Data
    order_master_dict = defaultdict(dict)

    # Site Visits Master Data
    site_visit_dict = defaultdict(dict)

    # Images Master Data
    image_master_dict = defaultdict(dict)

    # Site Visits By Week Per Customer
    site_visits_by_week = defaultdict(lambda: defaultdict(int))

    # Orders by week Per Customer
    orders_by_week = defaultdict(lambda: defaultdict(list))


def Ingest(e, D):
    """
    Ingest event Data into Data Lake
    :param e: Event 
    :param D: Data Lake
    :return: None
    """
    # Record New/ Update Customer events
    if e['type'] == 'CUSTOMER':
        D.customer_master_dict[e['key']] = {'last_name': e['last_name'], 'event_time': parser.parse(e['event_time']), 'adr_city': e['adr_city'], 'adr_state': e['adr_state']}

    # Record site visit events
    elif e['type'] == 'SITE_VISIT':
        D.site_visit_dict[e['key']] = {'event_time': parser.parse(e['event_time']), 'customer_id': e['customer_id'], 'tags': e['tags']}

        # Add Site Visits per week by Customer. Converting timestamp into epoch and loading into site_visits_by_week dict.
        D.site_visits_by_week[int((parser.parse(e['event_time']) - datetime(1970, 1, 1).replace(tzinfo=pytz.utc)).total_seconds()/(3600 * 24 *7))][e['customer_id']] += 1

    # Record image upload events
    elif e['type'] == 'IMAGE':
        D.image_master_dict[e['key']] = {'event_time': parser.parse(e['event_time']), 'customer_id': e['customer_id'], 'camera_make': e['camera_make'], 'camera_model': e['camera_model']}

    # Record New/Update Order Events
    elif e['type'] == 'ORDER':
        if not D.order_master_dict[e['key']] and e['verb'] != 'UPDATE':
            D.order_master_dict[e['key']] = {'event_time': parser.parse(e['event_time']), 'customer_id': e['customer_id'], 'total_amount': float(e['total_amount'][:-3].strip())}

            # Load orders per week per customer into a dict. Convert timestamp into epoch and load into orders_by_week
            if D.orders_by_week[int((parser.parse(e['event_time']) - datetime(1970, 1, 1).replace(tzinfo=pytz.utc)).total_seconds()/(3600 * 24 * 7))][e['customer_id']] is None:
                D.orders_by_week[int((parser.parse(e['event_time']) - datetime(1970, 1, 1).replace(tzinfo=pytz.utc)).total_seconds() / (3600 * 24 * 7))][
                    e['customer_id']] = []
                D.orders_by_week[int((parser.parse(e['event_time']) - datetime(1970, 1, 1).replace(tzinfo=pytz.utc)).total_seconds() / (3600 * 24 * 7))][
                    e['customer_id']].append(e['key'])
            else:
                D.orders_by_week[int((parser.parse(e['event_time']) - datetime(1970, 1, 1).replace(tzinfo=pytz.utc)).total_seconds() / (3600 * 24 * 7))][
                        e['customer_id']].append(e['key'])
        else:
            D.order_master_dict[e['key']] = {'event_time': parser.parse(e['event_time']),
                                             'customer_id': e['customer_id'],
                                             'total_amount': float(e['total_amount'][:-3].strip())}


def adder(flat_map, customers, D, flag):
    """
    
    :param flat_map: Flat Dictionary used for orders and site_visit totals per customer
    :param customers: Dictionary of customers and their orders or site_visits
    :param D: Data Lake
    :param flag: True indicates orders, False indicates site visits 
    :return: None
    """
    for customer, items in customers.items():
        if flag:
            flat_map[customer] += reduce(lambda x, y: x+y, map(lambda x: D.order_master_dict[x]['total_amount'], items))
        else:
            flat_map[customer] += items



def TopXSimpleLTVCustomers(x, D):
    order_map = defaultdict(float)
    site_map = defaultdict(int)
    topLtv = list
    map(lambda x: adder(order_map, x[1], D, True), D.orders_by_week.items())
    map(lambda x: adder(site_map, x[1], D, False), D.site_visits_by_week.items())


    return list(sorted(map(lambda x: (D.customer_master_dict[x[0]]['last_name'], 52 * (x[1]/ site_map[x[0]] ) *site_map[x[0]] * 10), order_map.items()), key=lambda x: x[1], reverse=True))[:x]



def main():
    data_lake = Data()

    event_list = json.load(open(os.path.join(os.path.dirname(__file__), "../input/input.txt")))
    map(lambda x: Ingest(x, data_lake), event_list)

    """
    Test Input has 3 customers, Smith, Woody and Buzz making multiple site visits and orders.
    
    
    >>> TopXSimpleLTVCustomers(1, data_lake)
    [(u'Smith', 12833.599999999999)]
    
     >>> TopXSimpleLTVCustomers(2, data_lake)
    [(u'Smith', 12833.599999999999), (u'Woody', 6416.799999999999)]
    
    >>> TopXSimpleLTVCustomers(10, data_lake)
    [(u'Smith', 12833.599999999999), (u'Woody', 6416.799999999999), (u'Buzz', 4336.8)]

        
    """
    with open(os.path.join(os.path.dirname(__file__), "../output/output.txt"), 'wb') as outfile:
        outfile.write('\n'.join('%s , %s' % x for x  in TopXSimpleLTVCustomers(3, data_lake)))

if __name__  == "__main__":
    import doctest
    doctest.testmod(extraglobs={'TopXSimpleLTVCustomers': main()})