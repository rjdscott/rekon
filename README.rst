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
* PyPi: https://pypi.org/project/rekon



How to use this library
--------------------------

Set-up virtualenv and import rekon

.. code-block:: console

    $ pip install --user virtualenv
    $ virtualenv .env
    $ pip install rekon

Reconciliation inputs:

.. code-block:: python

    # import rekon library
    from rekon import rekon

    # instantiate an reconciliation instance
    rec = rekon.Reconciliation()

    # load sample data from package
    rec.load_sample_data()

    # run reconciliation on first column in col mapping and use sqlite db in memory
    rec.reconcile(rec_col=1, sqlite_db=":memory:")

    # view results (in pretty format)
    rec.rec_results_pretty

    # to output results to a zip and open file location
    rec.output_report(output_dir='~/Desktop/EXAMPLE_OUTPUT',
                      file_name='rec-file',
                      output_format='zip',
                      open_file=True)

    # to output results to a spreadsheet (i.e. 'xlsx') and open file location
    rec.output_report(output_dir='~/Desktop/EXAMPLE_OUTPUT',
                      file_name='rec-file',
                      output_format='xlsx',
                      open_file=True)
