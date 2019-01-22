package main

import (
	"bufio"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/alpacahq/marketstore/executor"
	"github.com/alpacahq/marketstore/plugins/bgworker"
	"github.com/alpacahq/marketstore/utils"
	mio "github.com/alpacahq/marketstore/utils/io"
)

//HistoricRate  is a structure to hold historical data
type HistoricRate struct {
	Time   time.Time
	Low    float64
	High   float64
	Open   float64
	Close  float64
	Volume float64
}

//ByTime array of historical rates
type ByTime []HistoricRate

func (a ByTime) Len() int           { return len(a) }
func (a ByTime) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByTime) Less(i, j int) bool { return a[i].Time.Before(a[j].Time) }

//FetcherConfig is the configuration for GdFetcher you can define in
// marketstore's config file through bgworker extension.
type FetcherConfig struct {
	// list of currency symbols, defults to ["BTC", "ETH", "LTC", "BCH"]
	Symbols []string `json:"symbols"`
	// time string when to start first time, in "YYYY-MM-DD HH:MM" format
	// if it is restarting, the start is the last written data timestamp
	// otherwise, it starts from an hour ago by default
	QueryStart string `json:"query_start"`
	//we expect fetchTime to be hh:mm format
	FetchTime string `json:"fetch_at"`
}

// GdFetcher is the main worker instance.  It implements bgworker.Run().
type GdFetcher struct {
	config     map[string]interface{}
	symbols    []string
	queryStart time.Time
	fetchTime  time.Time
}

func recast(config map[string]interface{}) *FetcherConfig {
	data, _ := json.Marshal(config)
	ret := FetcherConfig{}
	json.Unmarshal(data, &ret)
	return &ret
}

// NewBgWorker returns the new instance of GdFetcher.  See FetcherConfig
// for the details of available configurations.
func NewBgWorker(conf map[string]interface{}) (bgworker.BgWorker, error) {
	symbols := []string{"TATASTEEL", "RELIANCE"}

	config := recast(conf)
	if len(config.Symbols) > 0 {
		symbols = config.Symbols
	}
	var queryStart time.Time
	if config.QueryStart != "" {
		trials := []string{
			"2006-01-02 03:04:05",
			"2006-01-02T03:04:05",
			"2006-01-02 03:04",
			"2006-01-02T03:04",
			"2006-01-02",
		}
		for _, layout := range trials {
			qs, err := time.Parse(layout, config.QueryStart)
			if err == nil {
				queryStart = qs.In(utils.InstanceConfig.Timezone)
				break
			}
		}
	}

	timeToFetch := getFetchTime(config.FetchTime)

	return &GdFetcher{
		config:     conf,
		symbols:    symbols,
		queryStart: queryStart,
		fetchTime:  timeToFetch,
	}, nil
}

func getFetchTime(givenTime string) time.Time {
	today := time.Now()
	year, month, day := today.Date()
	loc, _ := time.LoadLocation("Asia/Kolkata")
	timeToFetch := time.Date(year, month, day, 21, 30, 0, 0, loc)
	if givenTime != "" {
		units := strings.Split(givenTime, ":")
		hh, _ := strconv.Atoi(units[0])
		mm, _ := strconv.Atoi(units[1])
		timeToFetch = time.Date(year, month, day, hh, mm, 0, 0, loc)
	}
	return timeToFetch
}

func getSleepDuration(nextExpected time.Time) time.Duration {

	fmt.Println("fetchTime calculated is ", nextExpected)

	now := time.Now()
	duration := nextExpected.Sub(now)

	if duration < time.Second {
		nextExpected = nextExpected.Add(time.Duration(1) * time.Hour * 24)
		duration = nextExpected.Sub(now)
	}

	fmt.Println("current Time is ", now)
	fmt.Println("nextExpected run is at ", nextExpected)
	fmt.Println("sleeping for Duration of ", duration)

	return duration
}

// Run runs forever to get public historical rate for each configured symbol,
// and writes in marketstore data format.  In case any error including rate limit
// is returned from GD, it waits for a minute.
func (gd *GdFetcher) Run() {
	// This Method runs for ever
	for {
		gdfCSVReader(getNSE50Symbols())
		time.Sleep(getSleepDuration(gd.fetchTime))
	}
}

func main1() {

	gdfCSVReader(getNSE50Symbols())
	return

	for i, file := range readFiles() {
		fmt.Println("processing ...", file, i)
		processedFile := strings.Replace(file.Name, "csv", "processed", 1)

		os.Rename(file.Name, processedFile)
	}
}

// MyFile file type
type MyFile struct {
	Name string
	date time.Time
}

// MyFiles files type
type MyFiles []MyFile

func (p MyFiles) Len() int {
	return len(p)
}

func (p MyFiles) Less(i, j int) bool {
	return p[i].date.Before(p[j].date)
}

func (p MyFiles) Swap(i, j int) {
	p[i], p[j] = p[j], p[i]
}

func readFiles() MyFiles {
	//var listOfFiles []string

	var listOfFiles MyFiles

	files, err := ioutil.ReadDir("csv")
	if err != nil {
		log.Fatal(err)
	}

	for _, f := range files {
		//fmt.Println(f.Name())
		if f.Name() == ".DS_Store" {
			continue
		}

		fileName := f.Name()
		withFolder := "csv/" + f.Name()
		d1 := fileName[strings.LastIndex(fileName, "_")+1 : strings.Index(fileName, ".")]
		t1, _ := time.Parse("02012006", d1)
		file := MyFile{
			withFolder,
			t1,
		}
		listOfFiles = append(listOfFiles, file)
	}

	sort.Sort(listOfFiles)
	return listOfFiles
}
func dummyGDFcsvReader(symbols []string) {
	for i, f := range readFiles() {
		fmt.Println("Processing ", f.Name, i)

		csvFile, _ := os.Open(f.Name)
		defer csvFile.Close()
		reader := csv.NewReader(bufio.NewReader(csvFile))
		//Ticker,Date,Time,Open,High,Low,Close,Volume,Open Interest
		//20MICRONS.NSE,01/10/2018,09:15:59,43.2,43.6,43,43,1754,0

		var symbol, symbolToSave string
		var recordCount int
		var symbolCount int

		for {
			record, error := reader.Read()
			if error == io.EOF {
				break
			} else if error != nil {
				log.Fatal(error)
			}

			if record[0] == "Ticker" {
				symbol = record[0]
				continue
			}

			if strings.Count(record[0], ".") != 1 {
				continue
			}

			if symbol != record[0] {
				// found a new symbol

				i := sort.SearchStrings(symbols, symbolToSave)
				if i < len(symbols) && symbols[i] == symbolToSave {
					symbolCount++
				}

				symbol = record[0]
				symbolToSave = strings.Split(record[0], ".")[0]
				recordCount = 0
			}

			recordCount = recordCount + 1
		}

		fmt.Println("Total number of symbols = ", symbolCount)
		processedFile := strings.Replace(f.Name, "csv", "processed", 1)

		defer os.Rename(f.Name, processedFile)
	}

}

func gdfCSVReader(symbols []string) {

	for i, f := range readFiles() {
		fmt.Println("Processing ", f.Name, i)

		csvFile, _ := os.Open(f.Name)
		reader := csv.NewReader(bufio.NewReader(csvFile))

		//Ticker,Date,Time,Open,High,Low,Close,Volume,Open Interest
		//20MICRONS.NSE,01/10/2018,09:15:59,43.2,43.6,43,43,1754,0

		var symbol, symbolToSave string
		var recordCount int
		var symbolCount int

		var prices []HistoricRate
		interestedSymbol := false
		ignoredSymbol := false

		for {
			record, error := reader.Read()
			if error == io.EOF {
				break
			} else if error != nil {
				log.Fatal(error)
			}

			if record[0] == "Ticker" {
				symbol = record[0]
				continue
			}

			if strings.Count(record[0], ".") != 1 {
				continue
			}

			if symbol != record[0] {
				if len(prices) > 1 {
					savePrices(symbolToSave, prices)
					prices = nil
				}

				symbol = record[0]
				symbolToSave = strings.Split(record[0], ".")[0]
				ignoredSymbol = false
				interestedSymbol = false
				recordCount = 0
			}

			if ignoredSymbol == false && interestedSymbol == false {
				i := sort.SearchStrings(symbols, symbolToSave)
				if i < len(symbols) && symbols[i] == symbolToSave {
					interestedSymbol = true
					symbolCount++
				} else {
					ignoredSymbol = true
				}
			}

			if ignoredSymbol {
				continue
			}

			prices = appendPrice(record, prices)
			recordCount = recordCount + 1

		}
		fmt.Println("Total number of symbols = ", symbolCount)
		processedFile := strings.Replace(f.Name, "csv", "processed", 1)

		csvFile.Close()
		defer os.Rename(f.Name, processedFile)
	}
}

func appendPrice(record []string, prices []HistoricRate) []HistoricRate {
	var price HistoricRate

	t1, err := time.Parse("02/01/2006 15:04:05 (MST)", record[1]+" "+record[2]+" (IST)")
	open, err := strconv.ParseFloat(record[3], 64)
	high, err := strconv.ParseFloat(record[4], 64)
	low, err := strconv.ParseFloat(record[5], 64)
	close, err := strconv.ParseFloat(record[6], 64)
	volume, err := strconv.ParseFloat(record[7], 64)

	if err == nil {
		price = HistoricRate{t1, open, high, low, close, volume}
	} else {
		fmt.Print(err)
	}
	return append(prices, price)
}

func savePrices(symbol string, prices []HistoricRate) {
	fmt.Printf("Total records for symbol %s are %d \n", symbol, len(prices))

	epoch := make([]int64, 0)
	open := make([]float64, 0)
	high := make([]float64, 0)
	low := make([]float64, 0)
	close := make([]float64, 0)
	volume := make([]float64, 0)

	for _, rate := range prices {
		epoch = append(epoch, rate.Time.Unix())
		open = append(open, float64(rate.Open))
		high = append(high, float64(rate.High))
		low = append(low, float64(rate.Low))
		close = append(close, float64(rate.Close))
		volume = append(volume, rate.Volume)
	}

	cs := mio.NewColumnSeries()
	cs.AddColumn("Epoch", epoch)
	cs.AddColumn("Open", open)
	cs.AddColumn("High", high)
	cs.AddColumn("Low", low)
	cs.AddColumn("Close", close)
	cs.AddColumn("Volume", volume)

	csm := mio.NewColumnSeriesMap()
	tbk := mio.NewTimeBucketKey(symbol + "/" + "1Min" + "/OHLCV")
	csm.AddColumnSeries(*tbk, cs)
	executor.WriteCSM(csm, false)
}
