// https://www.nseindia.com/content/indices/ind_nifty50list.csv

package main

import (
	"encoding/csv"
	"fmt"
	"io"
	"log"
	"net/http"
	"sort"
)

func main() {
	dummyGDFcsvReader(getNSE50Symbols())
	//getSleepDuration(getFetchTime("21:30"))
}

func getNSE50Symbols() []string {
	var symbols []string
	tryCount := 3

	for tryCount > 0 {
		response, err := http.Get("https://www.nseindia.com/content/indices/ind_nifty50list.csv")
		if err != nil {
			log.Fatal(err)
		} else {
			defer response.Body.Close()
			reader := csv.NewReader(response.Body)

			for {
				// read one row from csv
				record, err := reader.Read()
				if err == io.EOF {
					tryCount = 0
					break
				} else if err != nil {
					log.Fatal(err)
				}

				if len(record) > 2 {
					if record[2] == "Symbol" {
						continue
					}

					symbols = append(symbols, record[2])
				} else {
					fmt.Println("mall formed record", record)
					tryCount = tryCount - 1
					break
				}
			}
		}
	}
	if len(symbols) > 0 {
		sort.Strings(symbols)
	}
	fmt.Println("symbols found are", len(symbols))
	return symbols
}
