from activfinancial import *
from activfinancial.constants import *
import json
from inspect import getsourcefile
from pathlib import Path
import common
import os
import argparse
from threading import Timer
import time
import logging


# Setup the logging configuration
logging.basicConfig(level=logging.INFO,
                    format='[%(levelname)s] %(asctime)s %(message)s')
logger = logging.getLogger()

# Define the command-line arguments
parser = argparse.ArgumentParser(
    description='Monitor real-time market data to generate price alerts.')

parser.add_argument('--config', type=str,
                    help='Path to the configuration file.')
args = parser.parse_args()

# Set the configuration file path
if args.config:
    config_path = Path(args.config)
else:
    config_path = Path(os.path.dirname(os.path.abspath(
        getsourcefile(lambda: 0)))).joinpath('config.json')


def load_config(config_path):
    """
    Load the configuration file.

    Parameters:
        config_path (str): The path to the configuration file.
    """

    with open(config_path) as f:
        return json.load(f)



def start_session(config):
    session = common.connect_session(
        config['activCredentials']['activ_username'],
        config['activCredentials']['activ_password'])

    parameters = {}
    event_list = [market_data.EVENT_TYPE_TRADE,
                  market_data.EVENT_TYPE_TRADE_NON_REGULAR]
    parameters[FID_EVENT_TYPE_INCLUDE_FILTER] = event_list

    parameters_handle = session.register_subscription_parameters(parameters)

    return session, parameters_handle

# Function to configure the subscriptions


def configure_subscriptions(config):
    handler_by_alertID = {}
    handle_by_alertID = {}

    for alert in config['alerts']:
        logger.info(f'Subscribing to {alert["symbol"]}...')

        handler = common.SubscriptionHandlerAlert(alert)
        handler_by_alertID[alert['alertID']] = handler

        handle = session.subscribe(DATA_SOURCE_ACTIV,
                                   alert['symbol'],
                                   handler,
                                   parameters=parameters_handle)
        handle_by_alertID[alert['alertID']] = handle

    return handler_by_alertID, handle_by_alertID


# Function to create configuration file watch
def watch_config_file(config_path):
    global config_updated
    global config_modified_time
    global config_watch_timer

    if config_path.stat().st_mtime > config_modified_time:
        config_modified_time = config_path.stat().st_mtime
        config_updated = True

    config_watch_timer.run()


# Initialize the configuration update flags
config_updated = False
config_modified_time = config_path.stat().st_mtime

# Load the configuration file
config = load_config(config_path)

# Create a timer to watch the configuration file
config_watch_timer = Timer(5, watch_config_file, [config_path])
config_watch_timer.start()


# Start the ACTIV session
session, parameters_handle = start_session(config)


# Configure the subscriptions
handler_by_alertID, handle_by_alertID = configure_subscriptions(config)


# Loop to process the session and subscriptions
subscriptionError = False
try:
    while not subscriptionError:
        session.process()
        for handler in handler_by_alertID.values():
            if handler.error:
                subscriptionError = True
                break

        # Configuration file has been modified - stop subscriptions and restart
        if config_updated and not subscriptionError:
            logger.info(
                'Configuration file updated - restarting subscriptions...')
            for handle in handle_by_alertID.values():
                handle.close()
            handler_by_alertID.clear()
            handler_by_alertID.clear()

            config = load_config(config_path)
            handler_by_alertID, handle_by_alertID = configure_subscriptions(
                config)

            config_updated = False

except KeyboardInterrupt:
    pass
finally:
    for handle in handle_by_alertID.values():
        handle.close()
    config_watch_timer.cancel()
