package main

import (
	"fmt"
	"github.com/gocarina/gocsv"
	"github.com/imroc/req/v3"
	"log"
	"os"
	_ "reflect"
	"runtime"
	"sync"
)

type Data struct {
	// 1. Create a struct for storing CSV lines and annotate it with csv struct field tags
	Date                        string  `csv:"date" json:"date"`
	AdaptorFaultRate            int64   `csv:"adaptor_fault_rate" json:"adaptor_fault_rate"`
	Circuit                     string  `csv:"circuit" json:"circuit"`
	Datatotal                   int64   `csv:"datatotal" json:"datatotal"`
	Disconnect                  int64   `csv:"disconnect" json:"disconnect"`
	Fiberflapping               int64   `csv:"fiberflapping" json:"fiberflapping"`
	Fttxpacketdrop              int64   `csv:"fttxpacketdrop" json:"fttxpacketdrop"`
	Highcpu                     int64   `csv:"highcpu" json:"highcpu"`
	Highmem                     int64   `csv:"highmem" json:"highmem"`
	Hightemp                    int64   `csv:"hightemp" json:"hightemp"`
	InfoAverageCPU              float64 `csv:"info_average_cpu" json:"info_average_cpu"`
	InfoAverageDevicesCount2gQ1 int64   `csv:"info_average_devices_count2g_q1" json:"info_average_devices_count2g_q1"`
	InfoAverageDevicesCount2gQ2 int64   `csv:"info_average_devices_count2g_q2" json:"info_average_devices_count2g_q2"`
	InfoAverageDevicesCount2gQ3 int64   `csv:"info_average_devices_count2g_q3" json:"info_average_devices_count2g_q3"`
	InfoAverageDevicesCount2gQ4 int64   `csv:"info_average_devices_count2g_q4" json:"info_average_devices_count2g_q4"`
	InfoAverageDevicesCount5gQ1 int64   `csv:"info_average_devices_count5g_q1" json:"info_average_devices_count5g_q1"`
	InfoAverageDevicesCount5gQ2 int64   `csv:"info_average_devices_count5g_q2" json:"info_average_devices_count5g_q2"`
	InfoAverageDevicesCount5gQ3 int64   `csv:"info_average_devices_count5g_q3" json:"info_average_devices_count5g_q3"`
	InfoAverageDevicesCount5gQ4 int64   `csv:"info_average_devices_count5g_q4" json:"info_average_devices_count5g_q4"`
	InfoAverageDevicesReconnect float64 `csv:"info_average_devices_reconnect" json:"info_average_devices_reconnect"`
	InfoAverageFttxpower        float64 `csv:"info_average_fttxpower" json:"info_average_fttxpower"`
	InfoAverageMem              float64 `csv:"info_average_mem" json:"info_average_mem"`
	InfoAverageRssi2g           float64 `csv:"info_average_rssi_2g" json:"info_average_rssi_2g"`
	InfoAverageRssi5g           int64   `csv:"info_average_rssi_5g" json:"info_average_rssi_5g"`
	InfoAverageSnr2g            float64 `csv:"info_average_snr_2g" json:"info_average_snr_2g"`
	InfoAverageSnr5g            int64   `csv:"info_average_snr_5g" json:"info_average_snr_5g"`
	InfoAverageTemp             float64 `csv:"info_average_temp" json:"info_average_temp"`
	InfoAverageTxrxRate2g       int64   `csv:"info_average_txrx_rate_2g" json:"info_average_txrx_rate_2g"`
	InfoAverageTxrxRate5g       int64   `csv:"info_average_txrx_rate_5g" json:"info_average_txrx_rate_5g"`
	InfoBandsteering            int64   `csv:"info_bandsteering" json:"info_bandsteering"`
	InfoMaxFttxpower            int64   `csv:"info_max_fttxpower" json:"info_max_fttxpower"`
	InfoMinFttxpower            float64 `csv:"info_min_fttxpower" json:"info_min_fttxpower"`
	Internetdisconnect          int64   `csv:"internetdisconnect" json:"internetdisconnect"`
	Lanerror                    int64   `csv:"lanerror" json:"lanerror"`
	Lowfttxpower                int64   `csv:"lowfttxpower" json:"lowfttxpower"`
	Lowlanspeed                 int64   `csv:"lowlanspeed" json:"lowlanspeed"`
	Lowwifitxrx                 int64   `csv:"lowwifitxrx" json:"lowwifitxrx"`
	Model                       string  `csv:"model" json:"model"`
	Multiplenetwork             int64   `csv:"multiplenetwork" json:"multiplenetwork"`
	No5ghz                      int64   `csv:"no5ghz" json:"no_5_ghz"`
	Painscore                   int64   `csv:"painscore" json:"painscore"`
	Powerloss                   int64   `csv:"powerloss" json:"powerloss"`
	Reboot                      int64   `csv:"reboot" json:"reboot"`
	Serial                      string  `csv:"serial" json:"serial"`
	T3Wifi5gIssue               int64   `csv:"t3_wifi5g_issue" json:"t_3_wifi_5_g_issue"`
	Toomanydevices              int64   `csv:"toomanydevices" json:"toomanydevices"`
	Toomanyinterference2g       int64   `csv:"toomanyinterference2g" json:"toomanyinterference_2_g"`
	Toomanyinterference5g       int64   `csv:"toomanyinterference5g" json:"toomanyinterference_5_g"`
	Version                     string  `csv:"version" json:"version"`
	Verycloseap                 int64   `csv:"verycloseap" json:"verycloseap"`
}

type Result struct {
	WorkID int
	jobID  string
	Status int
}

func crawl(wId int, jobs <-chan Data, results chan<- Result, wg *sync.WaitGroup) {

	client := req.C().EnableForceHTTP1()
	client.SetRootCertsFromFile("/Users/oat/Desktop/internship/ca.crt", "/Users/oat/Desktop/internship/es01.crt")
	r := client.R().
		SetBasicAuth("elastic", "elastic").
		SetHeader("Content-Type", "application/json")

	for {

		select {
		case job := <-jobs:
			var resp, err = r.
				SetBody(job).
				Post("https://localhost:9200/antman_index/_doc")
			if err != nil {
				log.Fatal(err) //like panic service will stop
			}
			//body := resp.String()
			fmt.Println("status code : ", resp.StatusCode, "job => "+job.Circuit, "workerID => ", wId)
			go func() {
				results <- Result{Status: resp.StatusCode, WorkID: wId, jobID: job.Circuit}
			}()
		}
	}

	wg.Done()
}

func main() {

	var c = make(chan Data)
	var jobs = make(chan Data)
	var results = make(chan Result)

	wg := new(sync.WaitGroup)

	runtime.GOMAXPROCS(100)

	fileHandle, err := os.OpenFile("june.csv", os.O_RDWR|os.O_CREATE, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}
	defer fileHandle.Close()

	go func() {
		err = gocsv.UnmarshalToChan(fileHandle, c)
		if err != nil {
			log.Fatal(err)
		}
	}()

	for w := 1; w <= 300; w++ {
		wg.Add(300)
		go crawl(w, jobs, results, wg)
	}

	for i := range c {
		jobs <- i
	}

	//for a := 0; a < 22705828; a++ {
	//	result := <-results
	//	fmt.Println(result)
	//	fmt.Println(a)
	//}
	wg.Wait()
	//close(results)

}
