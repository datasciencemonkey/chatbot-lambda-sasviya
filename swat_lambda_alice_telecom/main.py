import time
import os
import logging
import pandas as pd
# import requests
import swat
from settings import settings

# Can use cloud watch
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

# A simple pandas setting to change how values are displayed
pd.set_option('display.float_format', lambda x: '%.2f' % x)

"""
Example Slots obtained by inspecting the response :-
This can be used for troubleshooting
{
  "currentIntent": {
    "slots": {
    "group_name": None,
    "metric_slot": "sales",
    "phone": "iphone",
    "us_state": None
    },
    "name": "getTransactionMetrics",
    "confirmationStatus": "None"
  },
  "bot": {
    "alias": "$LATEST",
    "version": "$LATEST",
    "name": "oh-bot"
  },
  "userId": "sagang",
  "invocationSource": "DialogCodeHook",
  "outputDialogMode": "Text",
  "messageVersion": "1.0",
  "sessionAttributes": {}
}
"""

# Main handler

def lambda_handler(event, context):
    os.environ['TZ'] = 'America/New_York'
    time.tzset()
    logger.debug('event.bot.name={}'.format(event['bot']['name']))
    return dispatch(event)


def dispatch(intent_request):
    intent_name = intent_request['currentIntent']['name']
    if intent_name == 'getTransactionMetrics':
        return get_response_from_viya(intent_request)


# --- Helpers that build all of the responses ---
def get_response_from_viya(intent_request):
    slots = intent_request['currentIntent']['slots']

    user, pswd = ('sasdemo02', "lnxsas")  # _config.login()
    host = settings['host']
    portnum = settings['portnum']  # REST port
    conn = swat.CAS(host, portnum, user, pswd, protocol='http')
    conn.setsessopt(caslib='Public')
    session_attributes = {}
    status = process_metrics_req(connect=conn, metric_slot=slots['metric_slot'],
                                 phone=slots['phone'],
                                 group_name=slots['group_name'],
                                 us_state=slots['us_state'])
    conn.close()
    return close(session_attributes, 'Fulfilled', {'contentType': 'PlainText',
                                                   'content': status})


def process_metrics_req(connect, metric_slot, phone, group_name=None, us_state=None):
    connect.loadactionset('fedSQL')
    out = connect.CASTable('SAMP_DATA')
    # logic to get the metric_slot args included
    if metric_slot.lower() == "fraud":
        where_clause1 = "fraud=1"
    else:
        where_clause1 = "fraud in (0,1)"
    # logic to get the phone args included
    if phone == "all":
        where_clause2 = ""
    else:
        where_clause2 = " and lower(device_name) like '%{}%'".format(phone.lower())
    # logic to get the us_state args included
    if us_state is not None and us_state.lower() in [i.strip().lower() for i in list(out['state_full'].unique()) if
                                                     len(i) > 0]:
        where_clause3 = " and lower(state_full) = '{}'".format(us_state.lower())
    elif us_state is not None and us_state.lower() in [i.strip().lower() for i in list(out['state'].unique()) if
                                                       len(i) > 0]:
        where_clause3 = " and lower(state) = '{}'".format(us_state.lower())
    else:
        where_clause3 = ""

    if group_name is None:
        # if no by group processing is requested
        # noinspection SqlDialectInspection
        query = "SELECT sum(checkout_total) as sales, count(1) as orders " \
                "FROM Public.samp_data  WHERE {}{}{}".format(where_clause1,
                                                             where_clause2,
                                                             where_clause3)
        result = connect.fedSQL.execDirect(query)
        return_set = dict(sales=result['Result Set']['SALES'][0], orders=result['Result Set']['ORDERS'][0])
        return "There were a total of {:,} {} orders worth ${:,} at an AOV of ${:,} for {} devices {}".format(
            round(return_set['orders']),
            metric_slot,
            round(return_set['sales'], 2),
            round(return_set['sales'] /
                  return_set['orders'], 2),
            phone,
            '' if us_state is None else "in " + us_state)

    elif group_name is not None:
        return by_group_processing(cxn=connect, group=group_name.lower(),
                                   kw1=where_clause1, kw2=where_clause2,
                                   kw3=where_clause3)


def by_group_processing(cxn, group, kw1, kw2, kw3):
    group_var = 'device_name' if group == 'by phone' else 'state_full'
    query = f"""SELECT sum(checkout_total) as sales, count(1) as orders, {group_var}
                        FROM Public.samp_data WHERE {kw1}{kw2}{kw3} 
                        GROUP BY 3
                        ORDER BY 1 DESC"""
    result = cxn.fedSQL.execDirect(query)
    ret_df = result['Result Set']
    ret_df.columns = [i.lower() for i in list(ret_df.columns)]
    return_df = ret_df[ret_df.loc[:, group_var].str.strip() != '']
    return return_df

    # if group_name == 'by State':
    #     out_grp = out.groupby('state_full').summary(inputs=['checkout_total']
    #                                                 ).concat_bygroups()['Summary'][['N', 'Sum']]
    #     # Rename columns to properly present results
    #     out_grp.columns = ['Total Orders', 'Sales Total']
    #     # Sort by Max transactions
    #     result = out_grp.sort_values('Total Orders', ascending=False)
    # else:
    #     out_grp = out.groupby(['device_name'])
    #
    # return result
    # opts = dict(key, value)


def close(session_attributes, fulfillment_state, message):
    response = {
        'sessionAttributes': session_attributes,
        'dialogAction': {
            'type': 'Close',
            'fulfillmentState': fulfillment_state,
            'message': message
        }
    }
    return response
