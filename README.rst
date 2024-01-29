.. image:: https://img.shields.io/badge/-PyScaffold-005CA0?logo=pyscaffold
    :alt: Project generated with PyScaffold
    :target: https://pyscaffold.org/

|testBadge| |publishBadge|

.. |testBadge| image:: https://github.com/Dhali-org/Dhali-py/actions/workflows/package_test.yaml/badge.svg

.. |publishBadge| image:: https://github.com/Dhali-org/Dhali-py/actions/workflows/release.yaml/badge.svg

========
Dhali-py
========


This python package is intended to support interfacing with Dhali.
It offers a cli tools for creating payment claims used to access Dhali::

        $ dhali create-xrpl-payment-claim -h
        usage: dhali create-xrpl-payment-claim [-h] [-s SOURCE_SECRET] [-a AUTH_CLAIM_AMOUNT] [-t TOTAL_AMOUNT_CONTAINED_IN_CHANNEL]

        options:
        -h, --help            show this help message and exit
        -s SOURCE_SECRET, --source_secret SOURCE_SECRET
        -a AUTH_CLAIM_AMOUNT, --auth_claim_amount AUTH_CLAIM_AMOUNT
                                Amount (in drops) that claim authorises to be extracted from the channel (must be less than --total_amount_contained_in_channel)
        -t TOTAL_AMOUNT_CONTAINED_IN_CHANNEL, --total_amount_contained_in_channel TOTAL_AMOUNT_CONTAINED_IN_CHANNEL
                                Total drops to escrow in the channel (must be less than total amount of XRP in wallet)

Install and run
===============

1. `pip install .`
2. To generate a test XRPL wallet or determine you wallet's seed::

        dhali create-xrpl-wallet

3. To generate a Dhali payment claim::

        dhali create-xrpl-payment-claim -s <secret from 2.> -a 10000000 -t 100000000


.. _pyscaffold-notes:
