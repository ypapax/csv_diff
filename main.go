package main

import (
	"encoding/csv"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"io"
	"log"
	"os"
	"reflect"
	"strings"
)

const defaultColumnKeyNumber = 1

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if len(os.Args) != 3 {
		exec := "csv_diff"
		if len(os.Args) > 0 {
			exec = os.Args[0]
		}
		log.Fatalf("usage: %+v csvFilePath1 csvFilePath2", exec)
		return
	}
	file1 := os.Args[1]
	file2 := os.Args[2]
	lines1, err := csvToLines(file1)
	if err != nil {
		log.Fatalf("error: %+v", err)
	}
	lines2, err := csvToLines(file2)
	if err != nil {
		log.Fatalf("error: %+v", err)
	}
	if err := compareCsvLines(lines1, lines2, defaultColumnKeyNumber); err != nil {
		log.Fatalf("error: %+v", err)
	}

}

type lineItem struct {
	Symbol     string
	LineNumber int
	Line       []string
}

func compareCsvLines(csv1, csv2 [][]string, keyColumnNumber int) error {
	log.Printf("len diff: %+v", len(csv2)-len(csv1))
	shortestL := csv1
	longestL := csv2
	if len(csv2) < len(shortestL) {
		shortestL = csv2
		longestL = csv1
	}
	longestM, err := csvToMap(longestL, keyColumnNumber)
	if err != nil {
		return errors.WithStack(err)
	}

	var missingLines []lineItem
	var diffLines [][]lineItem
	var equalLines [][]lineItem

	for i, line := range shortestL {
		if len(line) <= keyColumnNumber {
			return errors.Errorf("not enough columns")
		}
		symbol := line[keyColumnNumber]
		first := lineItem{Line: line, LineNumber: i, Symbol: symbol}
		line2, ok := longestM[symbol]
		if !ok {
			missingLines = append(missingLines, first)
			continue
		}
		if reflect.DeepEqual(line, line2.Line) {
			second := line2
			equalLines = append(equalLines, []lineItem{first, second})
		} else {
			diffLines = append(diffLines, []lineItem{first, line2})
		}
	}
	log.Printf("diff lines: %+v", len(diffLines))
	log.Printf("missing lines: %+v", len(missingLines))
	log.Printf("equal lines: %+v", len(equalLines))
	return nil
}

func csvToMap(csv [][]string, keyColumnNumber int) (map[string]lineItem, error) {
	m := make(map[string]lineItem)
	for i, line := range csv {
		if len(line) <= keyColumnNumber {
			return nil, errors.Errorf("not enough columns")
		}
		k := line[keyColumnNumber]
		_, dupl := m[k]
		if dupl {
			return nil, errors.Errorf("duplicate key")
		}
		m[k] = lineItem{k, i, line}
	}
	return m, nil
}

func csvToLines(fileName string) (result [][]string, finalErr error) {
	b, err := os.ReadFile(fileName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	f := strings.NewReader(string(b))
	csvReader := csv.NewReader(f)
	lineNumber := 0
	for {
		lineNumber++
		line, errR := csvReader.Read()
		if errR == io.EOF {
			break
		}
		if errR != nil {
			return nil, errors.WithStack(errR)
		}
		// do something with read line
		logrus.Tracef("%+v line: %+v", lineNumber, strings.Join(line, ", "))
		//var date := line[0]
		result = append(result, line)
	}
	return result, nil
}
