# -*- coding: utf-8 -*-

"""Main module."""
import sqlite3 as sq
import pandas as pd
import os
from pkg_resources import resource_filename
import json


class Reconciliation(object):

    def __init__(self, system1_data=None, system2_data=None,
                 system_labels=('system1', 'system2'), column_mapping=None, row_mappings=None):

        self.system1_data = system1_data
        self.system2_data = system2_data
        self.system_labels = system_labels
        self.column_mappings = column_mapping
        self.row_mappings = row_mappings
        self.system1_norm_df = None
        self.system2_norm_df = None
        self.col_map = None
        self.col_map_inv = None
        self.col_map_norm = None
        self.rec_stats_dict = None
        self.rec_stats_json = None
        self.rec_result = None
        self.rec_result_pretty = None
        self.rec_msg = None

    def reconcile(self, rec_column=1, sqlite_db=":memory:"):
        '''
        This function performs the reconciliation of two data sets
            - it uses sqlite3 and pandas DataFrames for simplicity
            - it also assumes that all the data is provided in raw format but in DataFrames

        :param rec_column: int or str: Takes labels or integers corresponding to system1 labels
        :param sqlite_db: string: the location of the database, can be file or memory
        :return: DataFrame of reconciliation data
        '''

        # insert data into sqlite3 db in memory
        if sqlite_db == ":memory:":
            db_path = ":memory:"

        elif '/' in sqlite_db:
            db_path = sqlite_db

        else:
            db_path = (os.path.join(os.getcwd(), sqlite_db))

        # open a database context manager
        with sq.connect(db_path) as conn:

            # check to make sure that the system data is in correct format
            if isinstance(self.system1_data, pd.DataFrame) and isinstance(self.system2_data, pd.DataFrame):

                # normalise data and get ready to insert into database
                if self.column_mappings is None:
                    raise ValueError('No column mappings available, please try again.')

                else:

                    # we will only be taking in pandas DataFrames to begin with so lets check for it
                    if isinstance(self.column_mappings, pd.DataFrame):
                        self.col_map = dict(zip(self.column_mappings.ix[:, 0], self.column_mappings.ix[:, 1]))
                        self.col_map_inv = dict(zip(self.column_mappings.ix[:, 1], self.column_mappings.ix[:, 0]))
                        self.col_map_norm = {k: i for i, (k, v) in enumerate(self.col_map.items())}

                        # transform the system 2 DataFrames to prep for normalisation
                        self.system2_norm_df = self.system2_data.copy(deep=True)
                        self.system2_norm_df.rename(index=str, columns=self.col_map_inv, inplace=True)

                        # normalise both sys tem 1 and system 2 DataFrames
                        self.system1_norm_df = self.system1_data.copy(deep=True)
                        self.system1_norm_df.rename(index=str, columns=self.col_map_norm, inplace=True)
                        self.system2_norm_df.rename(index=str, columns=self.col_map_norm, inplace=True)

                        # add the data to sql database
                        self.system1_norm_df.to_sql(self.system_labels[0], con=conn, if_exists='replace', index=None)
                        self.system2_norm_df.to_sql(self.system_labels[1], con=conn, if_exists='replace', index=None)

                        self.row_mappings.to_sql('row_map', con=conn, if_exists='replace', index=None)
                        self.column_mappings.to_sql('col_map', con=conn, if_exists='replace', index=None)

                        # construct the sql for the reconciliation
                        sql = '''
                                select
                                    s1.'0' as s1_key,
                                    s2.'0' as s2_key,
                                    s1.'{col}' as s1_value{col},
                                    s2.'{col}' as s2_value{col},
                                    (s1.'{col}' - s2.'{col}') as s1s2_value{col}_diff
                                from system1 s1
                                left join row_map on s1.'0' = row_map.system1
                                left join system2 s2 on s2.'0' = row_map.system2;
                                '''.format(col=rec_column)

                        rec_df = pd.read_sql(sql, con=conn)

                        df_headers = [self.system_labels[0],
                                      self.system_labels[1],
                                      list(self.col_map.keys())[rec_column],
                                      list(self.col_map.values())[rec_column],
                                      'diff']

                        self.rec_result = rec_df.copy(deep=True)

                        self.rec_result_pretty = rec_df.copy(deep=True)
                        self.rec_result_pretty.columns = df_headers
                        self.rec_result_pretty.style.bar(subset=['diff'],
                                                         align='mid',
                                                         color=['#d65f5f', '#5fba7d'])

                        return self.rec_result_pretty

            else:

                raise ValueError("System data is not a Pandas DataFrame format, please try again.")

    def load_sample_data(self, echo=None):

        # get the system names
        self.system_labels = ['system1', 'system2']

        # get the mapping for rows and cols
        self.column_mappings = pd.read_csv(resource_filename('rekon', 'sample_data/col_mapping.csv'),
                                           header=None, names=self.system_labels)

        self.row_mappings = pd.read_csv(resource_filename('rekon', 'sample_data/row_mapping.csv'),
                                        header=None, names=self.system_labels)

        # get the system data (will use labels for now but can parse an index)
        self.system1_data = pd.read_csv(resource_filename('rekon', 'sample_data/system_1.csv'),
                                        usecols=self.column_mappings.ix[:, 0])

        self.system2_data = pd.read_csv(resource_filename('rekon', 'sample_data/system_2.csv'),
                                        usecols=self.column_mappings.ix[:, 1])

        if echo is not None:
            print(self.column_mappings)
            print(self.row_mappings)
            print(self.system1_data)
            print(self.system2_data)

        else:
            print("Sample data loaded...")

    def rec_stats(self, format='dict'):
        '''
        generates reconciliation statistics based on prior reconciliation
        :return: json with statistics
        '''
        num_rows = len(self.rec_result_pretty)
        num_breaks = self.rec_result_pretty['diff'].astype(bool).sum(axis=0)
        system1_misses = self.rec_result_pretty.iloc[:, 0].isna().sum()
        system2_misses = self.rec_result_pretty.iloc[:, 1].isna().sum()

        # create string representation of dictionary for stats storing as string

        self.rec_stats_dict = {
            'num rows': num_rows,
            'num breaks': num_breaks,
            str(self.system_labels[0]): system1_misses,
            str(self.system_labels[1]): system2_misses,
        }
        self.rec_stats_json = json.dumps({i: str(j) for i, j in self.rec_stats_dict.items()}, sort_keys=True, indent=4)

        if format == 'dict':
            return self.rec_stats_dict
        else:
            return self.rec_stats_json

    def update_row_mapping(self, row_mapping):
        '''
        allows user to update row mappings
        :param row_mapping: DataFrame for both system 1 and system 2
        :return: string containing success message
        '''
        pass

    def update_col_mapping(self, col_mapping):
        '''
        allows user to update column mappings
        :param col_mapping: DataFrame for both system 1 and system 2
        :return: string containing success message
        '''
        pass

    def output_rec_report(self, output_dir=None, output_format='xlsx'):
        '''
        outputs contents of reconciliation class to either xlsx, csv's or zip containing csv's
        :param output_dir: string for path to folder
        :param output_format: string: csv, xls, xlsx, zip
        :return: success message
        '''
        pass


if __name__ == '__main__':
    rec = Reconciliation()
    rec.load_sample_data()
    rec.reconcile()
    print(rec.rec_result)
    print(rec.rec_result_pretty)
