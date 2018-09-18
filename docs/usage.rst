=====
Usage
=====

To use rekon in a project:

.. code-block:: python

    from rekon import rekon

    # instantiate an reconciliation instance
    rec = rekon.Reconciliation()

    # load sample data
    rec.load_sample_data()

    # run reconciliation
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

