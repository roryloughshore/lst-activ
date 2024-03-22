import json
import os
import argparse
from pathlib import Path
from inspect import getsourcefile

from activfinancial import *
from activfinancial.constants import *


def connect_session(activ_username,
                    activ_password,
                    handler=None,
                    parameters={}):
    """
    Connect to the ACTIV server using the provided credentials.

        Parameters:
            activ_username (str): The username for the ACTIV server.
            activ_password (str): The password for the ACTIV server.
            handler (SessionHandler): The session handler to use.
            parameters (dict): Additional parameters to pass to the session.

        Returns:
            session: The session object.
    """

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


def print_field(name, field):
    """
    Helper function to print a field value with nice formatting.

        Parameters:
            name (str): The name of the field.
            field (Field): The field value.
    """

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


class SessionHandler:
    """
    A SessionHandler class with placeholder methods for handling
    session events.

    Attributes:
        None

    Methods:
        on_session_connect(session):
            Called for reconnects.
        on_session_disconnect(session):
            Called when a graceful disconnect has completed.
        on_session_error(session, status_code):
            Called when an error occurs.
        on_session_log_message(session, log_type, message):
            Called when a log message is received.

    """

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


class SnapshotHandler:
        def __init__(self):
                self.complete = False

        def on_snapshot(self, msg, context):
                self.complete = True

                print()
                print('on_snapshot:')
                print()
                print_field('DataSource', msg.data_source_id)
                print_field('Symbology', msg.symbology_id)
                print_field('Symbol', msg.symbol)
                print_field('PermissionId', msg.permission_id)
                print_field('UpdateId', msg.update_id)
                print()
                for (fid, field) in msg.fields.items():
                        field_name = context.session.metadata.get_field_name(msg.data_source_id, fid)
                        print_field(field_name, field)

        def on_snapshot_failure(self, msg, context):
                self.complete = True

                print()
                print('on_snapshot_failure:')
                print()
                print_field('DataSource', msg.data_source_id)
                print_field('Symbology', msg.symbology_id)
                print_field('Symbol', msg.symbol)
                print_field('StatusCode', status_code_to_string(msg.status_code))



class SnapshotHandlerTradeInfo:
    """
    A custom SnapshotHandler class for handling trade information.
    It will request then store the trade information for a symbol.

    Attributes:
        complete (bool): A flag indicating if the snapshot is complete.
        data (dict): A dictionary to store the trade information.

    Methods:
        on_snapshot(msg, context):
            Called when a snapshot is received.
        on_snapshot_failure(msg, context):
            Called when a snapshot request fails.
    """

    def __init__(self):
        self.complete = False
        self.data = {
            "LastReportedTrade": "",
            "LastReportedTradeSize": "",
            "LastReportedTradeTime": "",
            "LastReportedTradeDate": "",
            "Bid": "",
            "BidSize": "",
            "BidTime": "",
            "Ask": "",
            "AskSize": "",
            "AskTime": "",
            "TradeHigh": "",
            "TradeHighTime": "",
            "TradeLow": "",
            "TradeLowTime": "",
            "Open": "",
            "OpenTime": "",
            "PreviousClose": "",
            "PreviousOpen": "",
            "PreviousTradeHigh": "",
            "PreviousTradeLow": "",
            "PreviousPercentChange": "",
            "Currency": "",
            "Symbol": ""
        }

    def on_snapshot(self, msg, context):
        self.complete = True
        ask = ""
        ask_time = ""

        for (fid, field) in msg.fields.items():
            field_name = context.session.metadata.get_field_name(
                msg.data_source_id, fid)

            if field_name in self.data.keys():
                value = field.to_native()
                self.data[field_name] = value

    def on_snapshot_failure(self, msg, context):
        self.complete = True

        print()
        print("on_snapshot_failure:")
        print()
        print_field("DataSource", msg.data_source_id)
        print_field("Symbology", msg.symbology_id)
        print_field("Symbol", msg.symbol)
        print_field("StatusCode", status_code_to_string(msg.status_code))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description='''Get a snapshot of the current prices
            for a list of symbols.''')
    parser.add_argument('--config', type=str,
                        help='Path to the configuration file.')
    args = parser.parse_args()

    # Set the configuration file path
    if args.config:
        config_path = Path(args.config)
    else:
        config_path = Path(os.path.dirname(os.path.abspath(
            getsourcefile(lambda: 0)))).joinpath('config.json')

    # Load the configuration file
    with open(config_path) as f:
        config = json.load(f)

    # Set the list of symbols
    symbolList = config['symbols']

    # Connect to the ACTIV server
    session = connect_session(config['activCredentials']['activ_username'],
                              config['activCredentials']['activ_password'])

    handlerBySymbol = {}
    handleBySymbol = {}

    for symbol in symbolList:
        print('Requesting snapshot for %s...' % symbol)
        #handler = SnapshotHandlerTradeInfo()
        handler = SnapshotHandler()
        handlerBySymbol[symbol] = handler
        handle = session.snapshot(DATA_SOURCE_ACTIV, symbol, handler)
        handleBySymbol[symbol] = handle


    underlying_curent_prices = []
    snapshotComplete = False
    try:
        while not snapshotComplete:
            session.process()
            for handler in handlerBySymbol.values():
                snapshotComplete = True
                if not handler.complete:
                    snapshotComplete = False
                    break

    finally:
        # for handler in handlerBySymbol.values():
        #     underlying_curent_prices.append(
        #         [handler.data['Symbol'], handler.data['LastReportedTrade']])

        for handle in handleBySymbol.values():
            handle.close()

    # for underlying in underlying_curent_prices:
    #     print(underlying)
