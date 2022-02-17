package main

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
)

const filePath string = "IFF_911_Laimonas_Gaucas_dat_1.txt"
const resultsFilePath string = "results.txt"

// Flat is a struct made to hold data
type Flat struct {
	Street    string
	Number    int
	Rent      float64
	TotalRent float64
}

func (Flat *Flat) toString() string {
	return fmt.Sprintf("|%22v|%8v|%4.2f|%.2f|", Flat.Street, Flat.Number, Flat.Rent, Flat.TotalRent)
}

func main() {

	flats := ReadData(filePath)
	n := len(flats)
	years := 10

	group := sync.WaitGroup{}
	workers := sync.WaitGroup{}

	mainCh := make(chan Flat)
	dataCh := make(chan Flat)
	resultCh := make(chan Flat)
	filteredResultCh := make(chan []Flat)

	group.Add(2)
	go DataWorkerRoutine(n, mainCh, dataCh, &group)
	go ResultWorkerRoutine(n, resultCh, filteredResultCh, &group)

	workers.Add(6)
	for i := 0; i < 6; i++ {
		go WorkerRoutine(dataCh, resultCh, &workers, years)
	}

	for _, Flat := range flats {
		mainCh <- Flat
	}

	close(mainCh)
	workers.Wait()
	close(resultCh)

	filteredResults := <-filteredResultCh

	group.Wait()
	WriteResultsToFile(flats, filteredResults)
}

func ReadData(filepath string) []Flat {
	var flats []Flat

	file, err := os.Open(filepath)
	if err != nil {
		log.Fatal(err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)

	for scanner.Scan() {
		line := scanner.Text()
		parts := strings.Split(line, ";")

		num, err := strconv.Atoi(parts[1])
		rent, err := strconv.ParseFloat(parts[2], 64)

		if err != nil {
			log.Fatal(err)
		}

		Flat := Flat{
			Street: parts[0],
			Number: num,
			Rent:   rent,
		}

		flats = append(flats, Flat)
	}

	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}

	return flats
}

func WriteResultsToFile(originalData []Flat, results []Flat) {
	os.Remove(resultsFilePath)

	file, err := os.OpenFile(resultsFilePath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatal(err)
	}

	dataWriter := bufio.NewWriter(file)

	dataWriter.WriteString(fmt.Sprintf("%28v\n", "Original data"))
	dataWriter.WriteString(strings.Repeat("-", 43) + "\n")
	dataWriter.WriteString(fmt.Sprintf("|%3v|%22v|%8v|%6v|%8v|\n", "Id", "Street",
		"Number", "Rent", "Total rent"))
	dataWriter.WriteString(strings.Repeat("-", 43) + "\n")

	//original data
	for i := 0; i < len(originalData); i++ {
		dataWriter.WriteString(fmt.Sprintf("|%3v%34v\n", strconv.Itoa(i+1), originalData[i].toString()))
		dataWriter.WriteString(strings.Repeat("-", 43) + "\n")
	}

	//Skaiciuoja kiek yra ne tusciu flats
	var count = 0
	for _, a := range results {
		if a != (Flat{}) {
			count++
		}
	}

	dataWriter.WriteString("\n\n\n")

	dataWriter.WriteString(fmt.Sprintf("%25v\n", "Results"))
	if count > 0 {
		dataWriter.WriteString(strings.Repeat("-", 43) + "\n")
		dataWriter.WriteString(fmt.Sprintf("|%3v|%22v|%8v|%6v|%8v|\n", "Id", "Street", "Number", "Rent", "Total rent"))
		dataWriter.WriteString(strings.Repeat("-", 43) + "\n")

		// result data
		for i := 0; i < count; i++ {
			dataWriter.WriteString(fmt.Sprintf("|%3v%34v\n", strconv.Itoa(i+1), results[i].toString()))
			dataWriter.WriteString(strings.Repeat("-", 43) + "\n")
		}
	} else {
		dataWriter.WriteString(fmt.Sprintf("%12v%10v\n", " ", strings.Repeat("-", 19)))
		dataWriter.WriteString(fmt.Sprintf("%12v|%10v|\n", " ", "No Results Found!"))
		dataWriter.WriteString(fmt.Sprintf("%12v%10v\n", " ", strings.Repeat("-", 19)))
	}

	dataWriter.Flush()
	file.Close()
}

func DataWorkerRoutine(n int, mainCh <-chan Flat, dataCh chan<- Flat, group *sync.WaitGroup) {
	defer close(dataCh)
	defer group.Done()

	data := make([]Flat, n/2)

	var index = 0
	var removedCount = 0
	var addedCount = 0
	var done = false

	for !done {
		if index < n/2 && addedCount < n {
			var Flat = <-mainCh
			data[index] = Flat
			index++
			addedCount++
		} else if index != 0 && removedCount < n {
			dataCh <- data[index-1]
			data[index-1] = (Flat{})
			index--
			removedCount++
		} else {
			if removedCount == n {
				done = true
			}
		}
	}

}

func WorkerRoutine(dataCh <-chan Flat, resultCh chan<- Flat, group *sync.WaitGroup, years int) {
	defer group.Done()
	var months = years * 12

	for item := range dataCh {
		for i := 0; i < months; i++ {
			item.TotalRent = item.TotalRent + item.Rent
		}
		if item.Rent >= 200 && item.Rent <= 300 {
			resultCh <- item
		}
	}
}

func ResultWorkerRoutine(n int, resultCh <-chan Flat, filteredResultCh chan<- []Flat, group *sync.WaitGroup) {
	defer close(filteredResultCh)
	defer group.Done()

	flats := make([]Flat, n)
	var index = 0
	for item := range resultCh {
		var temp = 0
		if index == 0 {
			flats[index] = item
			index++
		} else if index > 0 {
			for i := 0; i < index; i++ {
				if item.Street < flats[i].Street || item.Street == flats[i].Street && item.Number < flats[i].Number {
					temp = i + 1
				}
			}
			for i := index; i > temp; i-- {
				flats[i] = flats[i-1]
			}
			flats[temp] = item
			index++

		}
	}

	filteredResultCh <- flats
}
