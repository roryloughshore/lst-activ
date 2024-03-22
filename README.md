# ACTIV Market Data
Pulls various market data feeds using the ACTIV market data API
A few examples have been created to show some of the capabilities of
this API.

When setting up each example the config-template.json file should be copied
to config.json and the parameters should be updated. At a minimum the
username and password for the ACTIV API will need to be entered here.

## equity_snapshot
This takes a list of equities and outputs a summary of the commonly
reference trade data fields.

## option_snapshot
This takes a list of equities and builds a list of all options
associated with them. This list is then filtered to look at options with
a strike price within 2% of the current price and which expire in the next
2-4 months. The trading data for these options is then downloaded and
saved.

## price_alert
This can be run as a service. It monitors one or more equity symbols and
triggers an alert based on a condition set in the configuration file.

