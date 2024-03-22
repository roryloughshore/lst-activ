import binascii
import os
from IPython.display import clear_output
from pprint import pprint
import requests
import json
import logging

from activfinancial import *
from activfinancial.constants import *


def connect_session(activ_username, activ_password, handler=None, parameters={}):
    if handler is None:
        handler = SessionHandler()

    # Enable downloading of all available dictionaries.
    parameters[FID_ENABLE_DICTIONARY_DOWNLOAD] = True

    session = Session(parameters, handler)

    connect_parameters = session.get_parameters()

    activ_host = "cg-ny4-l1.activfinancial.com:9011"

    connect_parameters[FID_HOST] = activ_host
    connect_parameters[FID_USER_ID] = activ_username
    connect_parameters[FID_PASSWORD] = activ_password

    print("Connecting to %s..." % connect_parameters[FID_HOST])
    session.connect(connect_parameters, 5000)

    return session


def print_field(name, field, clear=False):
    NAME_WIDTH = 40
    filler = "." * (NAME_WIDTH - len(name[:NAME_WIDTH - 1]))

    if isinstance(field, Field):
        if field.is_defined():
            string = str(field)
        else:
            string = "undefined"

        if not field.does_update_last:
            string += " *"
    elif field is None:
        return
    else:
        string = str(field)

    print("%s %s %s" % (name, filler, string))
    if clear:
        clear_output(wait=True)


def _get_event_type_string(data_source, event_type):
    activ_data_source_list = [
        DATA_SOURCE_ACTIV,
        DATA_SOURCE_ACTIV_DELAYED,
        DATA_SOURCE_ACTIV_LOW_LATENCY]

    if data_source in activ_data_source_list:
        return market_data.event_type_to_string(event_type)
    else:
        return str(event_type)


class SessionHandler:
    def on_session_connect(self, session):
        # Called for reconnects.
        print()
        print("on_session_connect")
        print()

    def on_session_disconnect(self, session):
        # Called when a graceful disconnect has completed.
        print()
        print("on_session_disconnect")
        print()

    def on_session_error(self, session, status_code):
        print()
        print("on_session_error:")
        print()
        print_field("StatusCode", status_code_to_string(status_code))

    def on_session_log_message(self, session, log_type, message):
        # Only print errors and warnings.
        if log_type == LOG_TYPE_ERROR or log_type == LOG_TYPE_WARNING:
            print()
            print("on_session_log_message:")
            print()
            print_field("LogType", log_type_to_string(log_type))
            print_field("Message", message)
        else:
            pass


class SubscriptionHandlerAlert:
    def __init__(self, alert):
        self.error = False
        self.alert = alert
        self.previous_trade = None
        self.alert_sent = False
        logging.basicConfig(format='[%(levelname)s] %(asctime)s %(message)s',
                            level=logging.INFO)
        self.logger = logging.getLogger()

    def send_alert(self):
        url = self.alert['notification']['endpoint']
        headers = {'content-type': 'application/json'}
        headers['authorization'] = self.alert['notification']['authorization']

        body = {}
        body['fromAddress'] = self.alert['notification']['from']['address']
        body['fromName'] = self.alert['notification']['from']['name']
        body['addAddresses'] = []
        for sendAddress in self.alert['notification']['sendTo']:
            body['addAddresses'].append(sendAddress)
        body['subject'] = self.alert['type']
        message = self.alert['symbol'] + " " + self.alert['condition'] + \
            " " + str(self.alert['value'])
        body['body'] = message

        body_json = json.dumps(body)

        response = requests.post(url, data=body_json, headers=headers)
        if response.status_code == 200:
            self.logger.info(f"Alert sent")
            self.alert_sent = True
        else:
            self.logger.error(
                f"Error sending alert - error code: {response.status_code} - {response.text}")

    def on_subscription_refresh(self, msg, context):
        pass

    def on_subscription_update(self, msg, context):
        trade = None
        alert_triggered = False

        for (fid, field) in msg.fields.items():
            field_name = context.session.metadata.get_field_name(
                msg.data_source_id, fid)
            if field_name == "Trade":
                trade = field.to_native()

        symbol = msg.symbol

        alert_message = ""

        if self.alert['condition'] == ">":
            if trade > self.alert['value']:
                alert_message = f"ALERT: {symbol} has traded above {self.alert['value']}"
                alert_triggered = True
        elif self.alert['condition'] == "<":
            if trade < self.alert['value']:
                alert_message = f"ALERT: {symbol} has traded below {self.alert['value']}"
                alert_triggered = True
        elif self.alert['condition'] == "cross above":
            if self.previous_trade is not None:
                if not self.previous_trade > self.alert['value'] and trade > self.alert['value']:
                    alert_message = f"ALERT: {symbol} has crossed above {self.alert['value']}"
                    alert_triggered = True
        elif self.alert['condition'] == "cross below":
            if self.previous_trade is not None:
                if not self.previous_trade < self.alert['price'] and trade < self.alert['price']:
                    alert_message = f"ALERT: {symbol} has crossed below {self.alert['value']}"
                    alert_triggered = True
        else:
            self.logger.error("Invalid alert condition")

        self.previous_trade = trade

        if not self.alert_sent and alert_triggered:
            self.logger.info(alert_message)
            self.send_alert()

    def on_subscription_topic_status(self, msg, context):
        pass

    def on_subscription_error(self, status_code, context):
        self.error = True

        self.logger.error("on_subscription_error:")
        self.logger.error("StatusCode: " + status_code_to_string(status_code))

    def on_subscription_failure(self, status_code, context):
        self.error = True

        self.logger.error("on_subscription_failure:")
        self.logger.error("StatusCode" + status_code_to_string(status_code))
