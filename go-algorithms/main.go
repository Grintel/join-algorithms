package main

// Copyright (c) Lukas Berger

import (
	"bufio"
	"fmt"
	"hash/fnv"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type Relation struct {
	Values []*uint32
}

func readFile(path string) (map[string][]*Relation, error) {
	file, err := os.Open(path)
	if err != nil {
		return map[string][]*Relation{}, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	hashMap := make(map[string][]*Relation)
	for scanner.Scan() {
		parseLine(scanner.Text(), hashMap)
	}
	return hashMap, nil
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func parseLine(line string, hashMap map[string][]*Relation) {
	values := strings.Split(line[0:len(line)-2], "\t")
	if len(values) == 3 {
		subject, property, object := values[0], values[1], values[2]
		subjectHash, objectHash := hash(subject), hash(object)
		hashMap[property] = append(hashMap[property], &Relation{
			Values: []*uint32{&subjectHash, &objectHash},
		})
	}
}

func HashJoin(table1, table2 []*Relation, index1, index2 int) ([]*Relation, error) {
	// build phase
	hashMap := make(map[uint32][]*Relation)
	for _, relation := range table1 {
		hashMap[*relation.Values[index1]] = append(hashMap[*relation.Values[index1]], relation)
	}

	// join phase
	outputTabe := []*Relation{}
	for _, relation := range table2 {
		if val, ok := hashMap[*relation.Values[index2]]; ok {
			for _, rel := range val {
				newValues := append(rel.Values, relation.Values...)
				outputTabe = append(outputTabe, &Relation{
					Values: newValues,
				})
			}

		}
	}
	return outputTabe, nil
}

func SortMergeJoin(table1, table2 []*Relation, index1, index2 int) ([]*Relation, error) {
	// sort relations

	var wg sync.WaitGroup

	wg.Add(1)
	go func(table1 []*Relation) {
		defer wg.Done()
		sort.Slice(table1, func(i, j int) bool {
			return *table1[i].Values[index1] < *table1[j].Values[index1]
		})
	}(table1)

	wg.Add(1)
	go func(table2 []*Relation) {
		sort.Slice(table2, func(i, j int) bool {
			return *table2[i].Values[index2] < *table2[j].Values[index2]
		})
		defer wg.Done()
	}(table2)

	wg.Wait()

	// merge relations
	var outputTable []*Relation
	i, j := 0, 0
	for i < len(table1) && j < len(table2) {
		if *table1[i].Values[index1] < *table2[j].Values[index2] {
			i++
		} else if *table1[i].Values[index1] > *table2[j].Values[index2] {
			j++
		} else {
			newValues := append(table1[i].Values, table2[j].Values...)
			outputTable = append(outputTable, &Relation{
				Values: newValues,
			})

			jPrime := j + 1
			for jPrime < len(table2) && *table1[i].Values[index1] == *table2[jPrime].Values[index2] {
				newValues := append(table1[i].Values, table2[jPrime].Values...)
				outputTable = append(outputTable, &Relation{
					Values: newValues,
				})
				jPrime++
			}

			iPrime := i + 1
			for iPrime < len(table1) && *table1[iPrime].Values[index1] == *table2[j].Values[index2] {
				newValues := append(table1[iPrime].Values, table2[j].Values...)
				outputTable = append(outputTable, &Relation{
					Values: newValues,
				})
				iPrime++
			}

			i++
			j++
		}
	}
	return outputTable, nil
}

func measureTimeSmallDataSet(joinFunc func(table1, table2 []*Relation, index1, index2 int) ([]*Relation, error), tables map[string][]*Relation) string {

	start := time.Now()
	newTable, err := joinFunc(tables["wsdbm:friendOf"], tables["wsdbm:follows"], 0, 1)
	if err != nil {
		return ""
	}
	log.Println("LEN-1: ", len(newTable))

	newTable, err = joinFunc(tables["wsdbm:likes"], newTable, 0, 1)
	if err != nil {
		return "err"
	}
	newTable, _ = joinFunc(tables["rev:hasReview"], newTable, 0, 1)
	if err != nil {
		return "err"
	}

	log.Println(len(newTable))
	return fmt.Sprintf("%s", time.Since(start))
}

func measureTimeBigDataSet(joinFunc func(table1, table2 []*Relation, index1, index2 int) ([]*Relation, error), tables map[string][]*Relation) string {

	start := time.Now()
	newTable, err := joinFunc(tables["<http://db.uwaterloo.ca/~galuc/wsdbm/friendOf>"],
		tables["<http://db.uwaterloo.ca/~galuc/wsdbm/follows>"], 0, 1)
	if err != nil {
		return ""
	}

	newTable, err = joinFunc(tables["<http://db.uwaterloo.ca/~galuc/wsdbm/likes>"], newTable, 0, 1)
	if err != nil {
		return "err"
	}

	/*
		Cannot execute this due to memory limitations :(

		log.Println("phase 2: ", len(newTable))
		newTable, _ = joinFunc(tables["<http://purl.org/stuff/rev#hasReview>"], newTable, 0, 1)
		if err != nil {
			return "err"
		}

		log.Println("phase 3: ", len(newTable))
	*/

	return fmt.Sprintf("%.5f", time.Since(start).Seconds())
}

func WriteToDisc(filename string, values []*uint32) {
	f, err := os.OpenFile(filename, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}

	defer f.Close()

	outputString := ""
	for i, val := range values {
		outputString += "%d, "
		if i != len(values)-1 {
			outputString += fmt.Sprintf("%d, ", *val)
		} else {
			outputString += fmt.Sprintf("%d\n", *val)
		}
	}

	if _, err = f.WriteString(outputString); err != nil {
		panic(err)
	}
}

func main() {
	tables, _ := readFile("../watdiv.10M.nt")
	for i := 0; i < 11; i++ {
		sortMergeTime := measureTimeBigDataSet(SortMergeJoin, tables)
		log.Println(sortMergeTime)
	}
}
