from collections import defaultdict
import hashlib
from time import sleep
from attr import has
import numpy as np

from typing import Dict

from pandas import array

def get_file_size(path: str) -> int:
    return sum(1 for line in open(path))

def get_tables(path: str) -> Dict[str, np.array]:
    file_size = get_file_size(path)
    string_dict = {}
    tables = {}
    with open(path, "r") as file:
        while True:
            line = file.readline()
            if not line:
                    break
            if len(line.split("\t")) == 3:
                subject, property, object = line.split("\t")
                object = object[:-3]
                subject_hash = get_hash(subject)
                property_hash = get_hash(property)
                object_hash = get_hash(object)
                string_dict[subject_hash] = subject
                string_dict[property_hash] = property
                string_dict[object_hash] = object
                if property not in tables:            
                    tables[property] = np.ndarray((1, 2), buffer=np.array([[subject_hash, object_hash]]), dtype=int)
                else:
                    tables[property] = np.append(tables[property], np.array([[subject_hash, object_hash]]), axis=0)
    return tables, string_dict

def get_hash(value: str) -> int:
    return int(hashlib.md5(value.encode("utf-8")).hexdigest(), 16)

def hash_join(table_1: np.ndarray, column_1: int, table_2 : np.ndarray, column_2: int, word_dict):
    # hash phase
    hash_map = dict()
    for x in table_1:
        key = x[column_1]
        if key not in hash_map:
            hash_map[key] = np.ndarray((1, 2), buffer=np.array([x]), dtype=int)
        else:
            hash_map[key] = np.append(hash_map[key], np.array([x]), axis=0)

    # join phase
    hash_map = {int(k): v for k, v in hash_map.items()}
    result = np.ndarray((1, 4), dtype=int)
    for row1 in table_2:
        key = row1[column_2] 
        if key in hash_map:
            for row2 in hash_map[key]:
                row = np.append(row1, row2)
                result = np.append(result, np.array([row]), axis=0)
    return result

if __name__ == "__main__":
    tables, string_dict = get_tables("100k.txt")
    follows_table = tables["wsdbm:follows"]
    friend_of_table = tables["wsdbm:friendOf"]
    
    print(len(friend_of_table))
    print(len(follows_table))
    
    print(hash_join(follows_table, 1, friend_of_table, 1, string_dict))