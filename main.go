package main

import (
	"encoding/csv"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
	"github.com/ypapax/logrus_conf"
	"io"
	"log"
	"os"
	"path"
	"reflect"
	"strings"
)

const defaultColumnKeyNumber = 1

func main() {
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	if err := logrus_conf.PrepareFromEnv("csv_diff"); err != nil {
		log.Fatalf("error: %+v", err)
	}
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
	size1, err := getFileSizeMegaBytes(file1)
	if err != nil {
		log.Fatalf("error: %+v", err)
	}
	size2, err := getFileSizeMegaBytes(file2)
	if err != nil {
		log.Fatalf("error: %+v", err)
	}
	log.Printf("size diff in mega bytes: %+v, size2: %+v, size2: %+v", size2-size1, size2, size1)
	lines1, err := csvToLines(file1)
	if err != nil {
		log.Fatalf("error: %+v", err)
	}
	lines2, err := csvToLines(file2)
	if err != nil {
		log.Fatalf("error: %+v", err)
	}
	if err := compareCsvLines(*lines1, *lines2, defaultColumnKeyNumber); err != nil {
		log.Fatalf("error: %+v", err)
	}

}

func getFileSizeMegaBytes(path string) (float64, error) {
	fi, err := os.Stat(path)
	if err != nil {
		return 0, errors.WithStack(err)
	}
	// get the size
	size := fi.Size()
	return float64(size) / 1024 / 1024, nil
}

type lineItem struct {
	Symbol     string
	LineNumber int
	Line       []string
	Filename   string
	Headers    []string
}

type csvFile struct {
	Lines    [][]string
	Headers  []string
	Filename string
}

func compareCsvLines(csv1, csv2 csvFile, keyColumnNumber int) error {
	log.Printf("len diff: %+v, len(csv2): %+v, len(csv1): %+v", len(csv2.Lines)-len(csv1.Lines), len(csv2.Lines), len(csv1.Lines))
	shortestL := csv1
	longestL := csv2
	if len(csv2.Lines) < len(shortestL.Lines) {
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

	for i, line := range shortestL.Lines {
		if len(line) <= keyColumnNumber {
			return errors.Errorf("not enough columns")
		}
		symbol := line[keyColumnNumber]
		first := lineItem{Line: line, LineNumber: i, Symbol: symbol, Filename: shortestL.Filename, Headers: shortestL.Headers}
		second, ok := longestM[symbol]
		if !ok {
			missingLines = append(missingLines, first)
			continue
		}
		if reflect.DeepEqual(line, second.Line) {
			equalLines = append(equalLines, []lineItem{first, second})
		} else {
			diffLines = append(diffLines, []lineItem{first, second})
		}
	}
	for i, ll := range diffLines {
		if len(ll) != 2 {
			return errors.Errorf("expected 2 elements")
		}
		first := ll[0]
		second := ll[1]
		lc := logrus.WithField("i", i).WithField("first.Symbol", first.Symbol).
			WithField("second.Symbol", second.Symbol)
		diffs, err := compareArrs(first, second)
		if err != nil {
			return errors.WithStack(err)
		}
		for j, d := range diffs {
			lcc := lc.WithField("j", j)
			lcc.Printf("%+v", d)
		}
	}
	log.Printf("diff lines: %+v", len(diffLines))
	log.Printf("missing lines: %+v", len(missingLines))
	log.Printf("equal lines: %+v", len(equalLines))
	return nil
}

type diffArr struct {
	Index        int
	Val1, Val2   string
	File1, File2 string
	Header       string
}

func compareArrs(li1, li2 lineItem) ([]diffArr, error) {
	arr1 := li1.Line
	arr2 := li2.Line
	if len(arr1) != len(arr2) {
		return nil, errors.Errorf("length should be equal")
	}
	var result []diffArr
	for i, v1 := range arr1 {
		v2 := arr2[i]
		if v1 == v2 {
			continue
		}
		var header = li1.Headers[i]
		result = append(result, diffArr{Index: i, Val1: v1, Val2: v2, File1: li1.Filename, File2: li2.Filename, Header: header})
	}
	return result, nil
}

func csvToMap(csv csvFile, keyColumnNumber int) (map[string]lineItem, error) {
	m := make(map[string]lineItem)
	for i, line := range csv.Lines {
		if len(line) <= keyColumnNumber {
			return nil, errors.Errorf("not enough columns")
		}
		k := line[keyColumnNumber]
		_, dupl := m[k]
		if dupl {
			return nil, errors.Errorf("duplicate key")
		}
		m[k] = lineItem{k, i, line, csv.Filename, csv.Headers}
	}
	return m, nil
}

func csvToLines(fileName string) (result *csvFile, finalErr error) {
	b, err := os.ReadFile(fileName)
	if err != nil {
		return nil, errors.WithStack(err)
	}
	f := strings.NewReader(string(b))
	csvReader := csv.NewReader(f)
	lineNumber := -1
	var headers []string
	var lines [][]string
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
		if lineNumber == 0 {
			headers = line
			continue
		}
		lines = append(lines, line)
	}
	fn := path.Base(fileName)
	return &csvFile{Lines: lines, Filename: fn, Headers: headers}, nil
}
