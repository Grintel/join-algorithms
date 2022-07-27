# Copyright (C) Lukas Berger 2022
import hashlib
from heapq import merge
from posixpath import split
import time
import numpy as np
from typing import Dict, List, Callable

# Determines at what factor the output table will be scaled down from worst case
INITAL_OUTPUT_TABLE_SIZE_FACTOR = 1000000

# Size of how many elemts should be allocated when increasing output table size
OUTPUT_TABLE_INCREMENT_SIZE = 1000000 

# Size of how many elemts should be allocated when increasing hash table size
HASH_TABLE_INCREMENT_SIZE = 10000

def get_tables(path: str, properties: List[str]) -> np.ndarray:
    """_summary_

    Args:
        path (str): _description_
        properties (List[str]): _description_

    Returns:
        np.ndarray: _description_
    """
    # used for translation of hash -> word
    string_dict = {}

    # stores all properties found
    tables = {}
    with open(path, "r") as file:
        while True:
            line = file.readline()
            if not line:
                    break
            if len(line.split("\t")) == 3:
                subject, property, object = line.split("\t")
                if property in properties:
                    # prune object because of strange formatting
                    object = object[:-3]

                    # hash all values to get integers
                    subject_hash = get_hash(subject)
                    object_hash = get_hash(object)

                    # store which hash belongs to which string
                    string_dict[subject_hash] = subject
                    string_dict[object_hash] = object

                    # build tables
                    if property not in tables:            
                        tables[property] = [[subject_hash, object_hash]]
                    else:
                        tables[property].append([subject_hash, object_hash])
    for key, value in tables.items():
        tables[key] = np.array(value, dtype=np.uint64)
    write_string_dict_to_file(string_dict=string_dict, path="stringHashs.txt")
    return tables

def write_string_dict_to_file(string_dict: Dict[int, str],path: str):
    with open(path, "a") as file:
        for key, value in string_dict.items():
            file.write(str(key) + "," + value + "\n")
            
            
def get_hash(value: str) -> int:
    """Generates an 16 digit md5 hash of the given value

    Args:
        value (str): some string

    Returns:
        int: integer hash of given string
    """
    return int(hashlib.md5(value.encode("utf-8")).hexdigest(), 16) % (2 ** 32)

def hash_join(table_1: np.ndarray, column_1: int, table_2 : np.ndarray, column_2: int) -> np.ndarray:
    """ Joins the given two tables on the given column indices by using the sort merge algorithm

    Args:
        table_1 (np.ndarray): Preferrably smaller table
        index_1 (int): Column of table_1 on which the join will be executed
        table_2 (np.ndarray): Preferrably larger table
        index_2 (int): Column of table_1 on which the join will be executed

    Returns:
        np.ndarray: New Table as the result of the join
    """
    # hash phase
    hash_map = dict()
    
    # store every occurance of join column and its row in a dict.
    for x in table_1:
        key = x[column_1]
        if key not in hash_map:
            # add a tuple of index and an empty array
            hash_map[key] = [0, np.empty((HASH_TABLE_INCREMENT_SIZE, table_1.shape[1]), dtype=np.uint64)]
            hash_map[key][1][0] = x
            hash_map[key][0] += 1
        
        else:
            current_index = hash_map[key][0]
            try:
                # if index is out of bound, allocate new memory
                hash_map[key][1][current_index] = x
                hash_map[key][0] += 1
            except IndexError:
                # allocate new empty memory
                hash_map[key][1] = np.concatenate((hash_map[key][1], 
                                                   np.empty((HASH_TABLE_INCREMENT_SIZE, table_1.shape[1] + table_2.shape[1]), dtype=np.uint64)),
                                                   dtype=np.uint64)
                hash_map[key][1][current_index] = x
                hash_map[key][0] += 1
                
    # prune tables
    for key, value in hash_map.items():
        index = value[0]
        hash_map[key][1] = hash_map[key][1][:index,:]

    # join phase
    
    # allocating the memory with an estimation that the size will be of a factor of the absolute worst case O(N*M)
    # fiddeling with this factor can help with ram problems
    n = table_1.shape[0]
    m = table_2.shape[0]
    output_table = np.zeros((int((n * m) / INITAL_OUTPUT_TABLE_SIZE_FACTOR), table_1.shape[1] + table_2.shape[1]),
                            dtype=np.uint64)
    
    i = 0
    for row1 in table_2:
        key = row1[column_2] 
        if key in hash_map:
            for row2 in hash_map[key][1]:
                split = len(row2)
                try:
                # If index is running out of bound, allocate new memory and join it to the output_table
                    output_table[i, :split] = row2
                    output_table[i, split:] = row1
                except IndexError:
                # allocate 1,000,000 new rows
                    output_table = np.concatenate((output_table, 
                                                np.zeros((OUTPUT_TABLE_INCREMENT_SIZE, table_1.shape[1] + table_2.shape[1]), dtype=np.uint64)),
                                                dtype=np.uint64)
                    output_table[i, :split] = row2
                    output_table[i, split:] = row1
                i += 1 
    return output_table[:i]

def sort_merge_join(table_1 : np.ndarray, index_1: int,
                    table_2: np.ndarray, index_2: int) -> np.ndarray:
    """ Joins the given two tables on the given column indices by using the sort merge algorithm

    Args:
        table_1 (np.ndarray): Preferrably smaller table
        index_1 (int): Column of table_1 on which the join will be executed
        table_2 (np.ndarray): Preferrably larger table
        index_2 (int): Column of table_1 on which the join will be executed

    Returns:
        np.ndarray: New Table as the result of the join
    """
    # sort values by their join column
    table_1 = table_1[table_1[:, index_1].argsort()]
    table_2 = table_2[table_2[:, index_2].argsort()]

    # merge values
    # initialize indices
    i = 0
    j = 0
    i_max = len(table_1) - 1
    j_max = len(table_2) - 1
    
    # allocating the memory with an estimation that the size will be some factor of the absolute worst case O(N*M)
    # fiddeling with this factor can help with ram problems
    output_table = np.zeros((int((i_max * j_max) / INITAL_OUTPUT_TABLE_SIZE_FACTOR), table_1.shape[1] + table_2.shape[1]),
                            dtype=np.uint64)
    count = 0
    split = table_1.shape[1]
    while i <= i_max and j <= j_max:
        if table_1[i][index_1] > table_2[j][index_2]:
            j += 1
        elif table_1[i][index_1] < table_2[j][index_2]:
            i += 1
        else:
            # match was found
            try:
                # If index is running out of bound, allocate new memory and join it to the output_table
                output_table[count, :split] = table_1[i]
                output_table[count, split:] = table_2[j]
            except IndexError:
                # allocate 1,000,000 new rows
                output_table = np.concatenate((output_table, 
                                              np.zeros((OUTPUT_TABLE_INCREMENT_SIZE, table_1.shape[1] + table_2.shape[1]), dtype=np.uint64)),
                                              dtype=np.uint64)
                output_table[count, :split] = table_1[i]
                output_table[count, split:] = table_2[j]
            count += 1
            #check if other columns of table2 match the value of table 1
            j_prime = j + 1
            while j_prime <= j_max and table_1[i][index_1] == table_2[j_prime][index_2]:
                try:
                    output_table[count, :split] = table_1[i]
                    output_table[count, split:] = table_2[j_prime]
                except IndexError:
                    output_table = np.concatenate((output_table,
                                                   np.zeros((OUTPUT_TABLE_INCREMENT_SIZE, table_1.shape[1] + table_2.shape[1]), dtype=np.uint64)),
                                                   dtype=np.uint64)
                    output_table[count, :split] = table_1[i]
                    output_table[count, split:] = table_2[j_prime]
                count += 1
                j_prime += 1
            
            #check if other columns of table1 match the value of table2
            i_prime = i + 1
            while i_prime <= i_max and table_1[i_prime][index_1] == table_2[j][index_2]:
                try:
                    output_table[count, :split] = table_1[i_prime]
                    output_table[count, split:] = table_2[j]
                except IndexError:
                    output_table = np.concatenate((output_table,
                                                   np.zeros((OUTPUT_TABLE_INCREMENT_SIZE, table_1.shape[1] + table_2.shape[1]), dtype=np.uint64)),
                                                  dtype=np.uint64)
                    output_table[count, :split] = table_1[i_prime]
                    output_table[count, split:] = table_2[j]
                count += 1
                i_prime += 1

            # increment indices
            i += 1
            j += 1
    return output_table[:count,:]


def merge_tables(merge_func: Callable[[np.ndarray, int, np.ndarray, int], np.ndarray],
                 tables: Dict[str, np.ndarray]) -> float:
    """ Merges the given tables as described in the exercise


    Args:
        merge_func (np.ndarray, int, np.ndarray, int], np.ndarray): Join Function
        tables (Dict[str, np.ndarray): Different Tables

    Returns:
        float: time elapsed in seconds
    """
    start = time.time()
    
    follows_table = tables["wsdbm:follows"]
    #follows_table = tables["<http://db.uwaterloo.ca/~galuc/wsdbm/follows>"]
    
    friend_of_table = tables["wsdbm:friendOf"]
    #friend_of_table = tables["<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>"]
    
    likes_table = tables["wsdbm:likes"]
    #likes_table = tables["<http://db.uwaterloo.ca/~galuc/wsdbm/likes>"]
    
    has_review_table = tables["rev:hasReview"]
    #has_review_table = tables["<http://purl.org/stuff/rev#hasReview>"]
    
    friend_follows = merge_func(friend_of_table, 0, follows_table, 1)

    merged = merge_func(friend_follows, 1, likes_table, 0)
    
    print(len(merged))
    merged = merge_func(has_review_table, 0, merged, 5)
    
    print("LENG: ", len(merged))
    return time.time() - start


if __name__ == "__main__":
    properties_big = ["<http://db.uwaterloo.ca/~galuc/wsdbm/follows>", "<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>",
                  "<http://db.uwaterloo.ca/~galuc/wsdbm/likes>", "<http://purl.org/stuff/rev#hasReview>"]
    properties_small = ["wsdbm:follows", "wsdbm:friendOf", "wsdbm:likes", "rev:hasReview"]
    tables = get_tables("100k.txt", properties=properties_small)
    print("TIME ELAPSED FOR MERGE JOIN: ", merge_tables(sort_merge_join, tables), "s")
    print("TIME ELAPSED FOR HASH JOIN: ", merge_tables(hash_join, tables), "s")
            