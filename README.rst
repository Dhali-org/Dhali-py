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
It offers a cli tools for creating payment claims used to access Dhali.

Install and run
===============

1. `pip install .`
2. To generate a test XRPL wallet or determine you wallet's seed::

        dhali create-xrpl-wallet

3. To generate a Dhali payment claim::

        dhali create-xrpl-payment-claim -s <secret from 2.> -a 10000000 -i <sequence number from 2.> -t 100000000


.. _pyscaffold-notes:
