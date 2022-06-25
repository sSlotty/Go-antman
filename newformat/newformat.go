package main

// Sample file for test: https://drive.google.com/file/d/1DFkJdX5UTnB_xL7g8xwkkdE8BxdurAhN/view?usp=sharing
import (
	"encoding/csv"
	"fmt"
	"log"

	// "github.com/imroc/req/v3"
	"io"

	"github.com/imroc/req/v3"
	"github.com/schollz/progressbar/v3"

	// "log"
	"os"
	"runtime"
	"strconv"
	"sync"
	"time"
)

type StructData struct {
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
	Status int
}

var mu sync.Mutex

func main() {

	f1, _ := os.Open("/Users/oat/Desktop/internship/Go-antman/june.csv")

	defer f1.Close()

	ts1 := time.Now()
	fmt.Println("start : ", ts1)
	concuRSwWP(f1)
	te1 := time.Now().Sub(ts1)

	// Read and Set to a map
	fmt.Println("END Concu: ", te1)
}

// with Worker pools
func concuRSwWP(f *os.File) {
	runtime.GOMAXPROCS(100)
	fcsv := csv.NewReader(f)
	rsRead := make([]*StructData, 0)
	numwpsRead := 1000
	jobsRead := make(chan []string)
	resRead := make(chan *StructData)

	numwpsSend := 100
	resultSend := make(chan *Result)
	var wg sync.WaitGroup

	workerRead := func(jobs <-chan []string, results chan<- *StructData) {
		for {
			select {
			case job, ok := <-jobs: // you must check for readable state of the channel.
				if !ok {
					return
				}
				results <- parseStruct(job)
			}
		}
	}

	wokrkerSend := func(wId int, jobs <-chan *StructData, results chan<- *Result) {

		client := req.C().EnableForceHTTP1()
		client.SetRootCertsFromFile("/Users/oat/Desktop/internship/ca.crt", "/Users/oat/Desktop/internship/es01.crt")
		r := client.R().
			SetBasicAuth("elastic", "elastic").
			SetHeader("Content-Type", "application/json")

		for job := range jobs {

			url := "https://localhost:9200/antman-index-" + job.Date + "/_doc"
			var resp, err = r.
				SetBody(job).
				Post(url)
			if err != nil {
				log.Fatal(err) //like panic service will stop
			}
			//body := resp.String()
			//fmt.Println("status code : ", resp.StatusCode, "wID => ", wId, " job => ", job.Circuit)
			results <- &Result{Status: resp.StatusCode, WorkID: wId}
		}

	}

	// init workers read
	for w := 0; w < numwpsRead; w++ {
		wg.Add(1)
		go func() {
			// this line will exec when chan `res` processed output at line 107 (func worker: line 71)
			defer wg.Done()
			workerRead(jobsRead, resRead)
		}()
	}

	//init worker send

	for x := 0; x < numwpsSend; x++ {

		wg.Add(numwpsSend)
		go wokrkerSend(x, resRead, resultSend)

	}

	go func() {
		for {
			rStr, err := fcsv.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				fmt.Println("ERROR: ", err.Error())
				break
			}

			jobsRead <- rStr
		}
	}()

	go func() {
		wg.Wait()
		//close(resRead)  // when you close(res) it breaks the below loop.
		//close(jobsRead) // close jobs to signal workers that no more job are incoming.

	}()

	bar := progressbar.Default(22705828)

	//var sa = 0
	wg.Add(1)
	for _ = range resultSend {
		bar.Add(1)

	}

	for r := range resRead {
		//fmt.Println(r)
		rsRead = append(rsRead, r)
	}

	fmt.Println("Count Concu ", len(rsRead))
}

//func printData(data []string, job string) {
//	obj := parseStruct(data)
//	js, _ := json.Marshal(obj)
//	fmt.Printf("\n[%v] ROW Id: %v - len %v", job, obj.OrderId, len(js))
//}

func parseStruct(data []string) *StructData {

	date, _ := time.Parse("20060102", data[0])
	adaptorFaultRate, _ := strconv.ParseInt(data[45], 10, 64)
	datatotal, _ := strconv.ParseInt(data[6], 10, 64)
	disconnect, _ := strconv.ParseInt(data[18], 10, 64)
	fiberflapping, _ := strconv.ParseInt(data[48], 10, 64)
	fttxpacketdrop, _ := strconv.ParseInt(data[21], 10, 64)
	highcpu, _ := strconv.ParseInt(data[13], 10, 64)
	highmem, _ := strconv.ParseInt(data[12], 10, 64)
	hightemp, _ := strconv.ParseInt(data[11], 10, 64)
	info_Averagecpu, _ := strconv.ParseFloat(data[32], 64)
	info_Average_device_count_2_gq1, _ := strconv.ParseInt(data[34], 10, 64)
	info_Average_device_count_2_gq2, _ := strconv.ParseInt(data[36], 10, 64)
	info_Average_device_count_2_gq3, _ := strconv.ParseInt(data[38], 10, 64)
	info_Average_device_count_2_gq4, _ := strconv.ParseInt(data[40], 10, 64)
	info_Average_device_count_5_gq1, _ := strconv.ParseInt(data[35], 10, 64)
	info_Average_device_count_5_gq2, _ := strconv.ParseInt(data[37], 10, 64)
	info_Average_device_count_5_gq3, _ := strconv.ParseInt(data[39], 10, 64)
	info_Average_device_count_5_gq4, _ := strconv.ParseInt(data[41], 10, 64)
	info_Average_device_reconnect, _ := strconv.ParseFloat(data[33], 64)
	info_Average_fttxpower, _ := strconv.ParseFloat(data[44], 64)
	info_Average_mem, _ := strconv.ParseFloat(data[31], 64)
	info_Average_rssi_2g, _ := strconv.ParseFloat(data[24], 64)
	info_Average_rssi_5g, _ := strconv.ParseInt(data[26], 10, 64)
	info_Average_snr_2g, _ := strconv.ParseFloat(data[25], 64)
	info_Average_snr_5g, _ := strconv.ParseInt(data[27], 10, 64)
	info_Average_temp, _ := strconv.ParseFloat(data[30], 64)
	info_Average_TxrxRate_2g, _ := strconv.ParseInt(data[28], 10, 64)
	info_Average_TxrxRate_5g, _ := strconv.ParseInt(data[29], 10, 64)
	info_Bandsteering, _ := strconv.ParseInt(data[23], 10, 64)
	info_max_fttxpower, _ := strconv.ParseInt(data[43], 10, 64)
	info_min_fttxpower, _ := strconv.ParseFloat(data[42], 64)
	internet_disconnect, _ := strconv.ParseInt(data[19], 10, 64)
	lanerror, _ := strconv.ParseInt(data[20], 10, 64)
	low_fttxpower, _ := strconv.ParseInt(data[8], 10, 64)
	low_land_speed, _ := strconv.ParseInt(data[10], 10, 64)
	low_wifi_txrx, _ := strconv.ParseInt(data[9], 10, 64)
	multiplenetwork, _ := strconv.ParseInt(data[47], 10, 64)
	no5ghz, _ := strconv.ParseInt(data[14], 10, 64)
	painscore, _ := strconv.ParseInt(data[5], 10, 64)
	powerloss, _ := strconv.ParseInt(data[49], 10, 64)
	reboot, _ := strconv.ParseInt(data[17], 10, 64)
	T3Wifi5gIssue, _ := strconv.ParseInt(data[22], 10, 64)
	toomanydevices, _ := strconv.ParseInt(data[7], 10, 64)
	toomanydevices_ference_2g, _ := strconv.ParseInt(data[15], 10, 64)
	toomanydevices_ference_5g, _ := strconv.ParseInt(data[16], 10, 64)
	Verycloseap, _ := strconv.ParseInt(data[46], 10, 64)
	return &StructData{
		Date:                        date.Format("2006-01-02 15:04:05")[0:10],
		AdaptorFaultRate:            adaptorFaultRate,
		Circuit:                     data[1],
		Datatotal:                   datatotal,
		Disconnect:                  disconnect,
		Fiberflapping:               fiberflapping,
		Fttxpacketdrop:              fttxpacketdrop,
		Highcpu:                     highcpu,
		Highmem:                     highmem,
		Hightemp:                    hightemp,
		InfoAverageCPU:              info_Averagecpu,
		InfoAverageDevicesCount2gQ1: info_Average_device_count_2_gq1,
		InfoAverageDevicesCount2gQ2: info_Average_device_count_2_gq2,
		InfoAverageDevicesCount2gQ3: info_Average_device_count_2_gq3,
		InfoAverageDevicesCount2gQ4: info_Average_device_count_2_gq4,
		InfoAverageDevicesCount5gQ1: info_Average_device_count_5_gq1,
		InfoAverageDevicesCount5gQ2: info_Average_device_count_5_gq2,
		InfoAverageDevicesCount5gQ3: info_Average_device_count_5_gq3,
		InfoAverageDevicesCount5gQ4: info_Average_device_count_5_gq4,
		InfoAverageDevicesReconnect: info_Average_device_reconnect,
		InfoAverageFttxpower:        info_Average_fttxpower,
		InfoAverageMem:              info_Average_mem,
		InfoAverageRssi2g:           info_Average_rssi_2g,
		InfoAverageRssi5g:           info_Average_rssi_5g,
		InfoAverageSnr2g:            info_Average_snr_2g,
		InfoAverageSnr5g:            info_Average_snr_5g,
		InfoAverageTemp:             info_Average_temp,
		InfoAverageTxrxRate2g:       info_Average_TxrxRate_2g,
		InfoAverageTxrxRate5g:       info_Average_TxrxRate_5g,
		InfoBandsteering:            info_Bandsteering,
		InfoMaxFttxpower:            info_max_fttxpower,
		InfoMinFttxpower:            info_min_fttxpower,
		Internetdisconnect:          internet_disconnect,
		Lanerror:                    lanerror,
		Lowfttxpower:                low_fttxpower,
		Lowlanspeed:                 low_land_speed,
		Lowwifitxrx:                 low_wifi_txrx,
		Model:                       data[3],
		Multiplenetwork:             multiplenetwork,
		No5ghz:                      no5ghz,
		Painscore:                   painscore,
		Powerloss:                   powerloss,
		Reboot:                      reboot,
		Serial:                      data[2],
		T3Wifi5gIssue:               T3Wifi5gIssue,
		Toomanydevices:              toomanydevices,
		Toomanyinterference2g:       toomanydevices_ference_2g,
		Toomanyinterference5g:       toomanydevices_ference_5g,
		Version:                     data[4],
		Verycloseap:                 Verycloseap,
	}
}
