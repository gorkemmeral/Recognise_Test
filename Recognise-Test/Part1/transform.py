import pandas as pd
import os

# Change directory for the output csv
os.chdir('/Users/gorkemmeral/Downloads/')

## Create a class to read input, transform data, and write output to local disc
class Transform:

    def __init__(self, app_filepath=None):

        self.app_filepath = app_filepath

    ## Read input csv file
    def read_csv_files(self, csv_filepath):
    
        return pd.read_csv(csv_filepath, header=0)
    
    ## Transform data
    def transform_data(self):

        app_df = self.read_csv_files(self.app_filepath)

        # Split string_agg into individual events
        stacked_df = pd.DataFrame(app_df.string_agg.str.split('|', expand=True, ).stack())[0].\
            apply(lambda x: str(x).replace(']', '=').replace('[', ''))
        stacked_df = pd.DataFrame(stacked_df)
        stacked_df.reset_index(inplace=True)
        stacked_df.set_index('level_0', inplace=True)
        stacked_df = stacked_df.rename(columns={0: 'data'})

        # Concat raw dataframe to stacked dataframe
        df_concat_stack = pd.concat([app_df, stacked_df], axis=1)

        # Transform timestamps as columns
        df_concat_stack['status'] = df_concat_stack['data'].apply(lambda x: str(x).split('=')[0])
        df_concat_stack['status_timestamps'] = df_concat_stack['data'].apply(lambda x: str(x).split('=')[1])

        # Select columns and convert timestamp format
        df_concat_stack = df_concat_stack[['UniqueID', 'account_type', 'status', 'status_timestamps']]
        df_concat_stack.reset_index(inplace=True)
        df_concat_stack = df_concat_stack[['UniqueID', 'status', 'status_timestamps']]
        df_concat_stack['status_timestamps'] = df_concat_stack['status_timestamps'].\
            apply(lambda x: pd.to_datetime(str(x), format='mixed').strftime("%b %d %Y, %H:%M:%S"))

        # Define a dictionary to store the data
        data_dict = {}

        for i, row in df_concat_stack.iterrows():
            uniqueID = row[df_concat_stack.columns.get_loc('UniqueID')]
            status = str(row[df_concat_stack.columns.get_loc('status')])
            status_ts = str(row[df_concat_stack.columns.get_loc('status_timestamps')])
            counter = 0

            # Create new key as uniqueID when there is no UniqueID value 
            if uniqueID not in data_dict.keys():
                data_dict[uniqueID] = {status + '_' + str(counter): status_ts}

            # When there is no status for UniqueID, create new key as status and assign timestamp value
            elif uniqueID in data_dict.keys() and status + '_' + str(counter) not in data_dict[uniqueID].keys():
                data_dict[uniqueID][status + '_' + str(counter)] = status_ts

            # Compare status to status keys, append counter to status, create new key as status_counter and assign timestamp value
            elif uniqueID in data_dict.keys() and status + '_' + str(counter) in data_dict[uniqueID].keys():
                key_list = []
                for k in data_dict[uniqueID].keys():
                    if status in str(k):
                        val = k.split('_')[-1]
                        key_list.append(val)

                counter = int(max(key_list)) + 1
                data_dict[uniqueID][status + '_' + str(counter)] = status_ts

        # Transpose final dataframe from dictionary
        final_df = pd.DataFrame(data_dict).transpose()
        final_df.reset_index(inplace=True)
        final_df = final_df.rename(columns={'index': 'UniqueID'})

        # Define columns 
        final_df = final_df[['UniqueID', 'REGISTERED_0', 'ACKNOWLEDGED_0', 'APPROVED_0', 'REACKNOWLEDGED_0',
                             'CLOSED_0', 'APPOINTMENT_SCHEDULED_0', 'REJECTED_0', 'ON_HOLD_0', 'BLOCKED_0',
                             'TERMINATE_0', 'INITIATED_0', 'APPROVED_1', 'ON_HOLD_1', 'INITIATED_1', 'REGISTERED_1',
                             'BLOCKED_1', 'CLOSED_1', 'APPROVED_2']]

        # Save output to local disc
        final_df.to_csv('Application Lifecycle Output'+ '.csv')

        return final_df
    
