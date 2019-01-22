// https://www.nseindia.com/content/indices/ind_nifty50list.csv

package main

import (
	"encoding/csv"
	"io"
	"log"
	"net/http"
	"sort"
)

func main() {
	//dummyGDFcsvReader(getNSE50Symbols())
	getSleepDuration(getFetchTime("21:30"))
}

func getNSE50Symbols() []string {
	response, err := http.Get("https://www.nseindia.com/content/indices/ind_nifty50list.csv")
	var symbols []string

	if err != nil {
		log.Fatal(err)
	} else {
		defer response.Body.Close()
		reader := csv.NewReader(response.Body)

		for {
			// read one row from csv
			record, err := reader.Read()
			if err == io.EOF {
				break
			} else if err != nil {
				log.Fatal(err)
			}
			if record[2] == "Symbol" {
				continue
			}

			symbols = append(symbols, record[2])
		}
	}
	sort.Strings(symbols)
	return symbols
}
