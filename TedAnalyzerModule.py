import pandas as pd
import ast


class TedAnalyzer:
    
    #Constructor method - reads a csv file and creates the database.
    def __init__(self, file_path):
        self.data = pd.read_csv(file_path)
    
    #Returns a copy of the dataframe
    def get_data(self):
        return self.data.copy(deep = True)
    
    #Returns a tuple with the shape of the database (num of rows and column)
    def get_data_shape(self):
        return self.data.shape
    
    #Returns the 'n' number of rows, sorted descendingly by 'column_name' value.
    #If 'n' is bigger than number of rows, returns all the dataframe.
    def get_top_n_by_col(self, column_name, n):
        if n > self.data.shape[0]: #checks if n is bigger than the dataframe number of rows.
            return self.data.copy(deep = True)
        else:
            #dataframe.'nlargest' - pandas built-in method to return the 'n' largest rows, sorted by vals of a specific column.
            return (self.data.nlargest(n, column_name)).copy(deep = True) 
    
    #Returns a list with the unique values that appears in column 'column_name'.
    def get_unique_values_as_list(self, column_name):
        return list(self.data[column_name].unique())
    
    #Returns a dict with the uniques values that appears in column 'column_name'.
    #Key - name of the value.
    #Value - number of appearances of val in dataframe.
    def get_unique_values_as_dict(self, column_name):
        val_as_dict = self.data[column_name].value_counts()#creates a series of the number of counts of each val.
        val_as_dict = val_as_dict.to_dict() #'series.to_dict', built-in method of pandas that creates a dict out of series.
        return val_as_dict
    
    #Returns a series with the number of 'null' values of each column.    
    def get_na_counts(self):
        return self.data.isna().sum()
    
    #Returns a copy of all the rows with at least 1 'null'
    def get_all_na(self):
        return (self.data[pd.isnull(self.data).any(axis=1)]).copy(deep = True)
    
    #Mutates the dataframe by deleting rows with at least 1 'null'. Resets index of rows after the removal.
    def drop_na(self):
        self.data = self.data.dropna().reset_index()
        return None
    
    #Returns a list of all the unique values in column 'tags'.   
    def get_unique_tags(self):
        ret_list = []
        new_tags = []
        column_length = self.get_data_shape()[0]#finds the number of rows
        for i in range(column_length):
            str_tags = self.data.at[i, "tags"]#assigns the value of 'tags' from each row
            new_tags = ast.literal_eval(str_tags)#Changes a string of list to a list.
            for j in new_tags:
                if j not in ret_list: #Checks for duplicate tags in the final list
                    ret_list.append(j)
        return ret_list

    #Mutates the dataframe by adding a new column of durations in minutes of each lecture
    def add_duration_in_minutes(self, new_column_name):
        self.data[new_column_name] = (self.data["duration"]/60).astype("int64")
        return None
        
    #Returns rows that their 'column_name' value is bigger than 'threshold'
    def filter_by_row(self, column_name, threshold):
        return self.data[self.data[column_name] >= threshold]