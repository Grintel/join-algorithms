import hashlib
from typing import Dict, Tuple, List

def get_file_size(path: str) -> int:
    return sum(1 for line in open(path))

def get_tables(path: str) -> Dict[str, List[Tuple[int, int]]]:
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
                    tables[property] = [(subject_hash, object_hash)]
                else:
                    tables[property].append((subject_hash, object_hash))
    return tables, string_dict

def get_hash(value: str) -> int:
    return int(hashlib.md5(value.encode("utf-8")).hexdigest(), 16)

def hash_join(table_1: List[Tuple[int, int]], column_1: int, table_2 : List[Tuple[int, int]], column_2: int):
    # hash phase
    hash_map = dict()
    for x in table_1:
        key = x[column_1]
        if key not in hash_map:
            hash_map[key] = [x]
        else:
            hash_map[key].append(x)

    # join phase
    result = []
    for row1 in table_2:
        key = row1[column_2] 
        if key in hash_map:
            for row2 in hash_map[key]:
                result.append((row1, row2))
    return result

def sort_merge_join(table_1 : List[Tuple[int, int]], index_1: int,
                    table_2: List[Tuple[int, int]], index_2: int) -> List[Tuple[int, int]]:
    # sort values
    table_1.sort(key=lambda x: x[index_1])
    table_2.sort(key=lambda x: x[index_2])

    # merge value
    i = 0
    j = 0
    i_max = len(table_1) - 1
    j_max = len(table_2) - 1
    output_table = []
    while i <= i_max and j <= j_max: 
        if table_1[i][index_1] > table_2[j][index_2]:
            j += 1
        elif table_1[i][index_2] < table_2[j][index_2]:
            i += 1
        else:
            output_table.append([table_1[i], table_2[j]])
            j_prime = j + 1
            while j_prime <= j_max and table_1[i][index_1] == table_2[j_prime][index_2]:
                output_table.append([table_1[i], table_2[j_prime]])
                j_prime += 1
            i_prime = i + 1
            while i_prime <= i_max and table_1[i_prime][index_1] == table_2[j_prime][index_2]:
                output_table.append([table_1[i_prime], table_2[j]])
                i_prime += 1
            i = i_prime
            j = j_prime
    return output_table
            



if __name__ == "__main__":
    tables, string_dict = get_tables("100k.txt")
    follows_table = tables["wsdbm:follows"]
    friend_of_table = tables["wsdbm:friendOf"]
    
    print(len(friend_of_table))
    print(len(follows_table))
    
    print(sort_merge_join(follows_table, 0, friend_of_table, 1))