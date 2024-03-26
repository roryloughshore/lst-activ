import os
import json
import csv
import argparse
from pathlib import Path
from datetime import datetime
from inspect import getsourcefile

from activfinancial import *
from activfinancial.constants import *
from tqdm import tqdm
import pandas as pd


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


class SnapshotHandlerOptionInfo:
    """
    A custom SnapshotHandler class for handling option information.
    It will request then store the option information for a symbol.

    Attributes:
        complete (bool): A flag indicating if the snapshot is complete.
        data (dict): A dictionary to store the option information.

    Methods:
        on_snapshot(msg, context):
            Called when a snapshot is received.
        on_snapshot_failure(msg, context):
            Called when a snapshot request fails.
    """

    def __init__(self):
        self.complete = False
        self.data = {
            "ClosingBid": "",
            "ClosingAsk": "",
            "Close": "",
            "Bid": "",
            "BidSize": "",
            "Ask": "",
            "AskSize": "",
            "OpenInterest": "",
            "Trade": "",
            "TradeSize": "",
            "CumulativeValue": "",
            "CumulativeVolume": "",
            "ExpirationDate": "",
            "OptionType": "",
            "StrikePrice": "",
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


class QueryHandler:
    """
    A custom QueryHandler class for handling query information.
    It will request and store a list of symbols which match the requirements
    specified in the associated query.

    Attributes:
        complete (bool): A flag indicating if the query is complete.
        symbols (list): A list to store the symbols which match the query.

    Methods:
        on_query_start(context):
            Called when a query starts.
        on_query_add(msg, context):
            Called when a symbol is added to the query results.
        on_query_remove(msg, context):
            Called when a symbol is removed from the query results.
        on_query_complete(context):
            Called when a query completes successfully.
        on_query_error(status_code, context):
            Called when a query fails with an error.
        on_query_failure(status_code, context):
            Called when a query fails with a failure.
    """

    def __init__(self):
        self.complete = False
        self.symbols = []

    def on_query_start(self, context):
        self.count = 0

    def on_query_add(self, msg, context):
        self.count += 1
        self.symbols.append(msg.symbol)

    def on_query_remove(self, msg, context):
        # Ignore.
        # Useful for watching changes but the samples exit on completion.
        pass

    def on_query_complete(self, context):
        self.complete = True
        print_field("TopicsFound", self.count)

    def on_query_error(self, status_code, context):
        self.complete = True

        print()
        print("on_query_error:")
        print()
        print_field("StatusCode", status_code_to_string(status_code))

    def on_query_failure(self, status_code, context):
        self.complete = True

        print()
        print("on_query_failure:")
        print()
        print_field("StatusCode", status_code_to_string(status_code))


if __name__ == '__main__':
    # Define the command line arguments
    parser = argparse.ArgumentParser(
        description='Get the details for options for a list of symbols.')

    parser.add_argument('--config', type=str,
                        help='Path to the configuration file.')
    args = parser.parse_args()

    # Set the configuration file path
    if args.config:
        config_path = Path(args.config)
    else:
        config_path = (Path(os.path.dirname(os.path.abspath(
            getsourcefile(lambda: 0)))).joinpath('config.json'))

    with open(config_path) as f:
        config = json.load(f)

    # Set the data file path for the option data
    data_dir = Path(os.path.dirname(os.path.abspath(
        getsourcefile(lambda: 0))))
    options_full_file = data_dir.joinpath('option_data.csv')
    options_filtered_file = data_dir.joinpath('option_data_filtered.csv')

    option_data_pd = pd.read_csv(options_full_file)
    option_data_pd['Check'] = 1

    # Set the list of symbols to update from the configuration file
    symbolList = config['symbols']

    # Connect to the ACTIV server
    session = connect_session(config['activCredentials']['activ_username'],
                              config['activCredentials']['activ_password'])

    # Empty dicts to store the handlers and handles used for
    # the snapshot requests
    handlerBySymbol = {}
    handleBySymbol = {}

    # For each symbol get the last traded price - used to filter the options
    # by strike price vs current price
    for symbol in symbolList:
        print('Requesting snapshot for %s...' % symbol)
        handler = SnapshotHandlerTradeInfo()
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
        for handler in handlerBySymbol.values():
            underlying_curent_prices.append(
                [handler.data['Symbol'], handler.data['LastReportedTrade']])

        for handle in handleBySymbol.values():
            handle.close()

    # Get a list of currently active options for each symbol,
    # compare this to the cached options data from the options_data.csv file,
    # remove any options that are no longer active then request the details
    # for any new options
    option_data = []  # New option data
    option_data_updated_list = []  # Updated currently active options

    for symbol in underlying_curent_prices:
        print('Requesting options symbols for %s...' % symbol[0])
        handler = QueryHandler()
        handle = session.query(
            DATA_SOURCE_ACTIV,
            'symbol=%s navigate=option' % symbol[0],
            handler)

        try:
            while not handler.complete:
                session.process()
        finally:
            option_symbols = handler.symbols
            handle.close()

        # Check for symbol overlap between currently active options
        # and cached options data
        # If there is overlap then add the cached option data to the
        # option_data_updated_list
        option_symbols_pd = pd.DataFrame(option_symbols,
                                         columns=['OptionSymbol'])

        option_data_pd_filtered = option_data_pd[
            option_data_pd['Underlying'] == symbol[0]]
        option_data_pd_filtered = (option_symbols_pd.merge(
            option_data_pd_filtered,
            how='inner',
            on='OptionSymbol')
            .drop(columns=['Check'])
            [['Underlying',
              'OptionSymbol',
              'OptionType',
              'StrikePrice',
              'ExpirationDate']])
        option_data_updated_list.append(option_data_pd_filtered)

        # Check for new symbols
        option_symbols_new = (option_symbols_pd.merge(option_data_pd[[
            'OptionSymbol', 'Check']],
            how='left',
            on='OptionSymbol')
            .query('Check.isna()'))

        if len(option_symbols_new) > 0:
            print("Getting option details for %s" % symbol)

            for option_symbol in tqdm(option_symbols_new.itertuples(),
                                      total=len(option_symbols_new)):

                handler = SnapshotHandlerOptionInfo()
                handle = session.snapshot(
                    DATA_SOURCE_ACTIV, option_symbol.OptionSymbol, handler)

                try:
                    while not handler.complete:
                        session.process()
                finally:
                    option_data.append([symbol[0],
                                        handler.data['Symbol'],
                                        handler.data['OptionType'],
                                        handler.data['StrikePrice'],
                                        handler.data['ExpirationDate']])
                    handle.close()

    # Combine the updated option data and the new option data
    if len(option_data) > 0:
        option_data_new_pd = pd.DataFrame(option_data, columns=[
            'Underlying',
            'OptionSymbol',
            'OptionType',
            'StrikePrice',
            'ExpirationDate'])
        option_data_updated_list.append(option_data_new_pd)

    option_data_output_pd = pd.concat(
        option_data_updated_list).sort_values(by='Underlying')
    option_data_output_pd['StrikePrice'] = option_data_output_pd[
        'StrikePrice'].astype(float)
    option_data_output_pd['ExpirationDate'] = pd.to_datetime(
        option_data_output_pd['ExpirationDate'])

    # Save the updated option data to the options_data.csv file
    option_data_output_pd.to_csv(options_full_file, index=False)

    # For each equity symbol get a list of options which have a
    # strike price within 2% of the current price
    # and an expiration date between 2 and 4 months from now
    option_data_filtered = []
    for symbol in underlying_curent_prices:
        print('Getting options for %s...' % symbol[0])
        current_price = float(symbol[1])

        option_symbol_filtered = option_data_output_pd[
            option_data_output_pd['Underlying'] == symbol[0]]

        option_symbol_filtered = option_symbol_filtered[
            (option_symbol_filtered['StrikePrice'] >= current_price * 0.95) &
            (option_symbol_filtered['StrikePrice'] <= current_price * 1.05) &
            (option_symbol_filtered['ExpirationDate'] >
                datetime.now()) &
            (option_symbol_filtered['ExpirationDate'] <=
                datetime.now() + pd.DateOffset(months=12))]

        if len(option_symbol_filtered) > 0:
            print("Getting filtered option details for %s" % symbol)

            for option_symbol in tqdm(option_symbol_filtered.itertuples(),
                                      total=len(option_symbol_filtered)):

                handler = SnapshotHandlerOptionInfo()
                handle = session.snapshot(
                    DATA_SOURCE_ACTIV, option_symbol.OptionSymbol, handler)

                try:
                    while not handler.complete:
                        session.process()
                finally:
                    option_data_current = [symbol[0],]
                    option_data_current.extend(handler.data.values())
                    option_data_filtered.append(option_data_current)
                    handle.close()

    # Save the filtered option data to the options_data_filtered.csv file
    option_data_filtered_pd = pd.DataFrame(
        option_data_filtered,
        columns=[
            'Underlying',
            'ClosingBid',
            'ClosingAsk',
            'Close',
            'Bid',
            'BidSize',
            'Ask',
            'AskSize',
            'OpenInterest',
            'Trade',
            'TradeSize',
            'CumulativeValue',
            'CumulativeVolume',
            'ExpirationDate',
            'OptionType',
            'StrikePrice',
            'OptionSymbol'
        ])
    option_data_filtered_pd.to_csv(options_filtered_file, index=False)
