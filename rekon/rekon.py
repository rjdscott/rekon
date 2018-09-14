# -*- coding: utf-8 -*-

"""Main module."""
import sqlite3 as sq
import pandas as pd
import os

# gather input files (import into sqlite table)
# gather mapping files (import into sqlite table)
# perform reconciliation (store in sqlite table)
# allow results output to file (txt, csv, excel)
# allow summary statistics
# allow custom user sql to be parsed for reconciliation
# allow user to open up table data in cli

# the user should be able to take everything in as data frames and parse them, we then will do all the work


class Reconciliation(object):

    def __init__(self, sys1_df=None, sys2_df=None,
                 system_labels=('system1', 'system2'), column_mapping=None, row_mappings=None):

        self.system1_df = sys1_df
        self.system2_df = sys2_df
        self.system_labels = system_labels
        self.column_mappings = column_mapping
        self.row_mappings = row_mappings
        self.system1_norm_df = None
        self.system2_norm_df = None
        self.col_map = None
        self.col_map_inv = None
        self.col_map_norm = None
        self.rec_stats = None
        self.rec_result = None
        self.rec_msg = None

    def __repr__(self):
        return self.rec_result

    def reconcile(self, rec_column=1, sqlite_db=':memory:'):
        '''
        This function performs the reconciliation of two data sets
            - it uses sqlite3 and pandas DataFrames for simplicity
            - it also assumes that all the data is provided in raw format but in DataFrames

        :param rec_column: int or str: Takes labels or integers corresponding to system1 labels
        :param sqlite_db: string: the location of the database, can be file or memory
        :return: DataFrame of reconciliation data
        '''

        # prepare to remap the sys2 df's columns using inverter dict
        # col_mapping_dict = dict(zip(self.column_mappings.ix[:, 1], self.column_mappings.ix[:, 0]))
        # self.system2_df.rename(index=str, columns=col_mapping_dict, inplace=True)

        # insert data into sqlite3 db in memory
        if sqlite_db == ':memory':
            conn = sq.connect(':memory:')

        elif '/' in sqlite_db:
            conn = sq.connect(sqlite_db)

        else:
            conn = sq.connect(os.path.join(os.getcwd(), sqlite_db))

        # check to make sure that the system data is in correct format
        if isinstance(self.system1_df, pd.DataFrame) and isinstance(self.system2_df, pd.DataFrame):

            # normalise data and get ready to insert into database
            print('Normalising tables...')

            if self.column_mappings is None:
                raise ValueError('No column mappings available, please try again.')

            else:

                # we will only be taking in pandas DataFrames to begin with so lets check for it
                if isinstance(self.column_mappings, pd.DataFrame):
                    self.col_map = dict(zip(self.column_mappings.ix[:, 0], self.column_mappings.ix[:, 1]))
                    self.col_map_inv = dict(zip(self.column_mappings.ix[:, 1], self.column_mappings.ix[:, 0]))
                    self.col_map_norm = {k: i for i, (k, v) in enumerate(self.col_map.items())}

                    # transform the system 2 DataFrames to prep for normalisation
                    self.system2_norm_df = self.system2_df.copy(deep=True)
                    self.system2_norm_df.rename(index=str, columns=self.col_map_inv, inplace=True)

                    # normalise both sys tem 1 and system 2 DataFrames
                    self.system1_norm_df = self.system1_df.copy(deep=True)
                    self.system1_norm_df.rename(index=str, columns=self.col_map_norm, inplace=True)
                    self.system2_norm_df.rename(index=str, columns=self.col_map_norm, inplace=True)

                    # add the data to sql database
                    self.system1_norm_df.to_sql(self.system_labels[0], con=conn, if_exists='replace', index=None)
                    self.system2_norm_df.to_sql(self.system_labels[1], con=conn, if_exists='replace', index=None)
                    print('normalised tables inserted to database')

                    self.row_mappings.to_sql('row_map', con=conn, if_exists='replace', index=None)
                    self.column_mappings.to_sql('col_map', con=conn, if_exists='replace', index=None)
                    print('Mappings added to database..')

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

                    self.rec_result = pd.read_sql(sql, con=conn)
                    self.rec_msg = 'Reconciliation successful!'

                    return self.rec_result
        else:
            raise ValueError("System data is not a Pandas DataFrame format, please try again.")

    def generate_rec_stats(self):
        pass

    def generate_output_file(self, file_format):
        pass

    def normalize_tables(self):
        # takes the two tables and normalises the headers

        if self.column_mappings is None:
            raise ValueError('No column mappings available, please try again.')

        else:

            # we will only be taking in pandas DataFrames to begin with so lets check for it
            if isinstance(self.column_mappings, pd.DataFrame):
                self.col_map = dict(zip(self.column_mappings.ix[:, 0], self.column_mappings.ix[:, 1]))
                self.col_map_inv = dict(zip(self.column_mappings.ix[:, 1], self.column_mappings.ix[:, 0]))
                self.col_map_norm = {k: i for i, (k, v) in enumerate(self.col_map.items())}

                # transform the system 2 DataFrames to prep for normalisation
                self.system2_norm_df = self.system2_df.copy(deep=True)
                self.system2_norm_df.rename(index=str, columns=self.col_map_inv, inplace=True)

                # normalise both sys tem 1 and system 2 DataFrames
                self.system1_norm_df = self.system1_df.copy(deep=True)
                self.system1_norm_df.rename(index=str, columns=self.col_map_norm, inplace=True)
                self.system2_norm_df.rename(index=str, columns=self.col_map_norm, inplace=True)

                print(self.system1_df)
                print(self.system1_norm_df)
                print(self.system2_df)
                print(self.system2_norm_df)

            else:
                raise ValueError('Column mappings needs to be a pandas DataFrame, please try again.')

    def load_test_data(self):

        # get the system names
        self.system_labels = ['system1', 'system2']

        # get the mapping for rows and cols
        self.column_mappings = pd.read_csv(os.path.join(os.getcwd(), 'rekon/sample_data/col_mapping.csv'), header=None, names=self.system_labels)
        # col_mapping_dict = pd.Series.from_csv('sample_data/col_mapping.csv', header=None).to_dict()
        # col_mapping_inverted_dict = {v: k for k, v in col_mapping_dict.items()}
        # col_normalization = {k: i for i, (k, v) in enumerate(col_mapping_dict.items())}

        self.row_mappings = pd.read_csv(os.path.join(os.getcwd(), 'rekon/sample_data/row_mapping.csv'), header=None, names=self.system_labels)

        # get the system data (will use labels for now but can parse an index)
        self.system1_df = pd.read_csv(os.path.join(os.getcwd(), 'rekon/sample_data/system_1.csv'), usecols=self.column_mappings.ix[:, 0])
        self.system2_df = pd.read_csv(os.path.join(os.getcwd(), 'rekon/sample_data/system_2.csv'), usecols=self.column_mappings.ix[:, 1])
        # self.system1_norm_df = pd.read_csv(os.path.join(os.getcwd(), 'rekon/sample_data/system_1.csv'), usecols=self.column_mappings.ix[:, 0])
        # self.system2_norm_df = pd.read_csv(os.path.join(os.getcwd(), 'rekon/sample_data/system_2.csv'), usecols=self.column_mappings.ix[:, 1])

        # rename column names in system 2 so they match the first system and can be confirmed
        # self.system2_df.rename(index=str, columns=col_mapping_inverted_dict, inplace=True)
        # self.system1_df.rename(index=str, columns=col_normalization, inplace=True)
        # self.system2_df.rename(index=str, columns=col_normalization, inplace=True)

        # now lets chuck the data into the database
        # conn = sq.connect(os.path.join(os.getcwd(), 'test.db'))
        # self.system1_df.to_sql(self.system_labels[0], conn, if_exists='replace', index=None, )
        # self.system2_df.to_sql(self.system_labels[1], conn, if_exists='replace', index=None)
        # self.column_mappings.to_sql('col_map', con=conn, if_exists='replace', index=None)
        # self.row_mappings.to_sql('row_map', con=conn, if_exists='replace', index=None)


def test_database():

    # get the system names
    sys_names = ['system1', 'system2']

    # get the mapping for rows and cols
    col_mapping_df = pd.read_csv('sample_data/col_mapping.csv', header=None, names=sys_names)
    col_mapping_dict = pd.Series.from_csv('sample_data/col_mapping.csv', header=None).to_dict()
    col_mapping_inverted_dict = {v: k for k, v in col_mapping_dict.items()}
    col_normalization = {k: i for i, (k, v) in enumerate(col_mapping_dict.items())}

    row_mapping_df = pd.read_csv('sample_data/row_mapping.csv', header=None, names=sys_names)

    # get the system data (will use labels for now but can parse an index)
    sys_1 = pd.read_csv('sample_data/system_1.csv', usecols=col_mapping_df.ix[:, 0])
    sys_2 = pd.read_csv('sample_data/system_2.csv', usecols=col_mapping_df.ix[:, 1])

    # rename column names in system 2 so they match the first system and can be confirmed
    sys_2.rename(index=str, columns=col_mapping_inverted_dict, inplace=True)
    sys_1.rename(index=str, columns=col_normalization, inplace=True)
    sys_2.rename(index=str, columns=col_normalization, inplace=True)

    # now lets chuck the data into the database
    conn = sq.connect(os.path.join(os.getcwd(), 'test.db'))
    sys_1.to_sql(sys_names[0], conn, if_exists='replace', index=None,)
    sys_2.to_sql(sys_names[1], conn, if_exists='replace', index=None)
    col_mapping_df.to_sql('col_map', con=conn, if_exists='replace', index=None)
    row_mapping_df.to_sql('row_map', con=conn, if_exists='replace', index=None)

    # run the rec and output full table
    sql = '''
        select
            s1.'0' as s1_key,
            s2.'0' as s2_key,
            s1.'1' as s1_value1,
            s2.'1' as s2_value1, 
            (s1.'1' - s2.'1') as s1s2_value1_diff
        from system1 s1
        join row_map on s1.'0' = row_map.system1
        join system2 s2 on s2.'0' = row_map.system2;
    '''
    rec_full_df = pd.read_sql(sql, con=conn)
    print(rec_full_df)

    # todo: convert sql row labels back to normal from dictionary invert
    # todo: terminate the database


def test_database2():

    # get the system names
    sys_names = ['system1', 'system2']

    # get the mapping for rows and cols
    col_mapping_df = pd.read_csv('sample_data/col_mapping.csv', header=None, names=sys_names)
    row_mapping_df = pd.read_csv('sample_data/row_mapping.csv', header=None, names=sys_names)

    # get the system data (will use labels for now but can parse an index)
    sys_1 = pd.read_csv('sample_data/system_1.csv', header=None)
    sys_2 = pd.read_csv('sample_data/system_2.csv', header=None)

    # now lets chuck the data into the database
    conn = sq.connect(os.path.join(os.getcwd(), 'test2.db'))
    sys_1.to_sql(sys_names[0], conn, if_exists='replace', index=None,)
    sys_2.to_sql(sys_names[1], conn, if_exists='replace', index=None)
    col_mapping_df.to_sql('col_map', con=conn, if_exists='replace', index=None)
    row_mapping_df.to_sql('row_map', con=conn, if_exists='replace', index=None)


if __name__ == '__main__':
    rec = Reconciliation()
    # rec.load_test_data()
    # rec.reconcile(rec_column=1, sqlite_db='test.db')
    # print(rec.rec_result)
    # rec.reconcile(rec_column=2, sqlite_db='test.db')
    # print(rec.rec_result)
