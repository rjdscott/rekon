=====
Usage
=====

To use rekon in a project::

    import rekon

    # instantiate a reconciliation instance
    rec = rekon.Reconciliation()

    # load sample data
    rec.load_sample_data()

    # run reconciliation
    rec.reconcile()

    # view results
    rec.rec_results()

