=====
rekon
=====


.. image:: https://img.shields.io/pypi/v/rekon.svg
        :target: https://pypi.python.org/pypi/rekon

.. image:: https://img.shields.io/travis/rjdscott/rekon.svg
        :target: https://travis-ci.org/rjdscott/rekon

.. image:: https://readthedocs.org/projects/rekon/badge/?version=latest
        :target: https://rekon.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




rekon provides a suite of reconciliation tools for operations and finance


* Free software: MIT license
* Documentation: https://rekon.readthedocs.io.



How to use this library
----------------------

Set-up virtualenv and import `rekon`::

    pip install --user virtualenv
    virtualenv .env
    pip install rekon

Reconciliation inputs::

    # instantiate reconciliation class object
    rec = Reconciliation(sys1_df, sys2_df, system_labels, col_mapping, row_mapping)

    # to run a reconciliation, call the reconcile method
    rec.reconcile(rec_col=1, sqlite_db=":memory:")

