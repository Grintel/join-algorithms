import hashlib
import time
from typing import Dict, Tuple, List, Callable

def get_tables(path: str) -> Tuple[Dict[str, List[Tuple[int, int]]], Dict[int, str]]:
    """ Reads the rdf triplet files and builds vertically tables out of every property found

    Args:
        path (str):  path to rdf file

    Returns:
        Tuple[Dict[str, List[Tuple[int, int]]], Dict[int, str]]: 
        First Item of tuple are the tables of the relations.
        Second Item is the hashmap of string values found in the file.
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
                # prune object because of strange formatting
                object = object[:-3]

                # hash all values to get integers
                subject_hash = get_hash(subject)
                property_hash = get_hash(property)
                object_hash = get_hash(object)

                # store which hash belongs to which string
                string_dict[subject_hash] = subject
                string_dict[property_hash] = property
                string_dict[object_hash] = object

                # build tables
                if property not in tables:            
                    tables[property] = [(subject_hash, object_hash)]
                else:
                    tables[property].append((subject_hash, object_hash))
    return tables, string_dict

def get_hash(value: str) -> int:
    """Generates an 16 digit md5 hash of the given value

    Args:
        value (str): some string

    Returns:
        int: integer hash of given string
    """
    return int(hashlib.md5(value.encode("utf-8")).hexdigest(), 16)

def hash_join(table_1: List[Tuple[int, int]], column_1: int, table_2 : List[Tuple[int, int]], column_2: int) -> List[Tuple[int]]:
    """ Joins the given two tables on the given column indices by using the sort merge algorithm

    Args:
        table_1 (List[Tuple[int, int]]): Preferrably smaller table
        index_1 (int): Column of table_1 on which the join will be executed
        table_2 (List[Tuple[int, int]]): Preferrably larger table
        index_2 (int): Column of table_1 on which the join will be executed

    Returns:
        List[Tuple[int, int]]: New Table as the result of the join
    """
    # hash phase
    hash_map = dict()
    
    # store every occurance of join column and its row in a dict.
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
                rows = [cell for cell in row2] + [cell for cell in row1]
                result.append(rows)
    return result

def sort_merge_join(table_1 : List[Tuple[int, int]], index_1: int,
                    table_2: List[Tuple[int, int]], index_2: int) -> List[Tuple[int, int]]:
    """ Joins the given two tables on the given column indices by using the sort merge algorithm

    Args:
        table_1 (List[Tuple[int, int]]): Preferrably smaller table
        index_1 (int): Column of table_1 on which the join will be executed
        table_2 (List[Tuple[int, int]]): Preferrably larger table
        index_2 (int): Column of table_1 on which the join will be executed

    Returns:
        List[Tuple[int, int]]: New Table as the result of the join
    """
    # sort values by their join column
    table_1.sort(key=lambda x: x[index_1])
    table_2.sort(key=lambda x: x[index_2])

    # merge values
    # initialize indices
    i = 0
    j = 0
    i_max = len(table_1) - 1
    j_max = len(table_2) - 1
    output_table = []
    while i <= i_max and j <= j_max: 
        if table_1[i][index_1] > table_2[j][index_2]:
            j += 1
        elif table_1[i][index_1] < table_2[j][index_2]:
            i += 1
        else:
            # match was found
            rows = [cell for cell in table_1[i]] + [cell for cell in table_2[j]]
            output_table.append(rows)

            #check if other columns of table2 match the value of table 1
            j_prime = j + 1
            while j_prime <= j_max and table_1[i][index_1] == table_2[j_prime][index_2]:
                rows = [cell for cell in table_1[i]] + [cell for cell in table_2[j_prime]]
                output_table.append(rows)
                j_prime += 1
            
            #check if other columns of table1 match the value of table2
            i_prime = i + 1
            while i_prime <= i_max and table_1[i_prime][index_1] == table_2[j][index_2]:
                rows = [cell for cell in table_1[i_prime]] + [cell for cell in table_2[j]]
                output_table.append(rows)
                i_prime += 1

            # increment indices
            i += 1
            j += 1

    return output_table


def merge_tables(merge_func: Callable[[List[Tuple[int]], int, List[Tuple[int]], int], List[Tuple[int]]],
                 tables: Dict[str, List[Tuple[int, int]]]) -> float:
    """ Merges the given tables as described in the exercise


    Args:
        merge_func (Callable[[List[Tuple[int]], int, List[Tuple[int]], int], List[Tuple[int]]]): Join Function
        tables (Dict[str, List[Tuple[int, int]]]): Different Tables

    Returns:
        float: time elapsed in seconds
    """
    start = time.time()
    follows_table = tables["wsdbm:follows"]
    friend_of_table = tables["wsdbm:friendOf"]
    likes_table = tables["wsdbm:likes"]
    has_review_table = tables["rev:hasReview"]
    friend_follows = merge_func(follows_table, 0, friend_of_table, 1)
    merged = merge_func(friend_follows, 3, likes_table, 0)
    merged = merge_func(merged, 5, has_review_table, 0)
    return time.time() - start


if __name__ == "__main__":
    tables, string_dict = get_tables("100k.txt")
    print("TIME ELAPSED FOR MERGE JOIN: ", merge_tables(hash_join, tables), "s")
    print("TIME ELAPSED FOR MERGE JOIN: ", merge_tables(sort_merge_join, tables), "s")