package main

import (
	"bytes"
	"crypto/rand"
	"flag"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	opRead  = "Read"
	opWrite = "Write"
)

var bufferBytes []byte

func main() {
	endpoint := flag.String("endpoint", "", "S3 endpoint(s) comma separated - http://IP:PORT,http://IP:PORT")
	accessKey := flag.String("accessKey", "", "the S3 access key")
	accessSecret := flag.String("accessSecret", "", "the S3 access secret")
	bucketName := flag.String("bucket", "bucketname", "the bucket for which to run the test")
	objectNamePrefix := flag.String("objectNamePrefix", "loadgen_test_", "prefix of the object name that will be used")
	objectSize := flag.Int64("objectSize", 80*1024*1024, "size of individual requests in bytes (must be smaller than main memory)")
	numClients := flag.Int("numClients", 40, "number of concurrent clients")
	numSamples := flag.Int("numSamples", 200, "total number of requests to send")
	flag.Parse()

	if *numClients > *numSamples || *numSamples < 1 {
		fmt.Printf("numClients(%d) needs to be less than numSamples(%d) and greater than 0\n", *numClients, *numSamples)
		os.Exit(1)
	}

	if *endpoint == "" {
		fmt.Println("You need to specify endpoint(s)")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Setup and print summary of the accepted parameters
	params := Params{
		requests:         make(chan Req),
		responses:        make(chan Resp),
		numSamples:       *numSamples,
		numClients:       uint(*numClients),
		objectSize:       *objectSize,
		objectNamePrefix: *objectNamePrefix,
		bucketName:       *bucketName,
		endpoints:        strings.Split(*endpoint, ","),
	}
	fmt.Println(params)
	fmt.Println()

	// Generate the data from which we will do the writting
	fmt.Printf("Generating in-memory sample data... ")
	timeGenData := time.Now()
	bufferBytes = make([]byte, *objectSize, *objectSize)
	buffer := bytes.NewBuffer(bufferBytes)
	_, err := io.CopyN(buffer, rand.Reader, *objectSize)
	if err != nil {
		fmt.Printf("Could not allocate a buffer")
		os.Exit(1)
	}
	fmt.Printf("Done (%s)\n", time.Since(timeGenData))
	fmt.Println()

	// Start the load clients and run a write test followed by a read test
	params.StartClients(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(*accessKey, *accessSecret, ""),
		Region:           aws.String("igneous-test"),
		S3ForcePathStyle: aws.Bool(true),
	})

	fmt.Printf("Running %s test...\n", opWrite)
	writeResult := params.Run(opWrite)
	fmt.Println()

	fmt.Printf("Running %s test...\n", opRead)
	readResult := params.Run(opRead)
	fmt.Println()

	// Repeating the parameters of the test followed by the results
	fmt.Println(params)
	fmt.Println()
	fmt.Println(writeResult)
	fmt.Println()
	fmt.Println(readResult)
}

func (params *Params) Run(op string) Result {
	startTime := time.Now()

	// Start submitting load requests
	go params.submitLoad(op)

	// Collect and aggregate stats for completed requests
	result := Result{opDurations: make([]float64, 0, params.numSamples), operation: op}
	for i := 0; i < params.numSamples; i++ {
		resp := <-params.responses
		errorString := ""
		if resp.err != nil {
			result.numErrors++
			errorString = fmt.Sprintf(", error: %s", resp.err)
		} else {
			result.bytesTransmitted = result.bytesTransmitted + params.objectSize
			result.opDurations = append(result.opDurations, resp.duration.Seconds())
		}
		fmt.Printf("%v operation completed in %0.2fs (%d/%d) - %0.2fMB/s%s\n",
			op, resp.duration.Seconds(), i+1, params.numSamples,
			(float64(result.bytesTransmitted)/(1024*1024))/time.Since(startTime).Seconds(),
			errorString)
	}

	result.totalDuration = time.Since(startTime)
	sort.Float64s(result.opDurations)
	return result
}

// Create an individual load request and submit it to the client queue
func (params *Params) submitLoad(op string) {
	bucket := aws.String(params.bucketName)
	for i := 0; i < params.numSamples; i++ {
		key := aws.String(fmt.Sprintf("%s%d", params.objectNamePrefix, i))
		if op == opWrite {
			params.requests <- &s3.PutObjectInput{
				Bucket: bucket,
				Key:    key,
				Body:   bytes.NewReader(bufferBytes),
			}
		} else if op == opRead {
			params.requests <- &s3.GetObjectInput{
				Bucket: bucket,
				Key:    key,
			}
		} else {
			panic("Developer error")
		}
	}
}

func (params *Params) StartClients(cfg *aws.Config) {
	for i := 0; i < int(params.numClients); i++ {
		cfg.Endpoint = aws.String(params.endpoints[i%len(params.endpoints)])
		go params.startClient(cfg)
	}
}

// Run an individual load request
func (params *Params) startClient(cfg *aws.Config) {
	svc := s3.New(session.New(), cfg)
	for request := range params.requests {
		putStartTime := time.Now()
		var err error
		numBytes := params.objectSize

		switch r := request.(type) {
		case *s3.PutObjectInput:
			req, _ := svc.PutObjectRequest(r)
			// Disable payload checksum calculation (very expensive)
			req.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
			err = req.Send()
		case *s3.GetObjectInput:
			req, resp := svc.GetObjectRequest(r)
			err = req.Send()
			numBytes = 0
			if err == nil {
				numBytes, err = io.Copy(ioutil.Discard, resp.Body)
			}
			if numBytes != params.objectSize {
				err = fmt.Errorf("expected object length %d, actual %d", params.objectSize, numBytes)
			}
		default:
			panic("Developer error")
		}

		params.responses <- Resp{err, time.Since(putStartTime), numBytes}
	}
}

// Specifies the parameters for a given test
type Params struct {
	operation        string
	requests         chan Req
	responses        chan Resp
	numSamples       int
	numClients       uint
	objectSize       int64
	objectNamePrefix string
	bucketName       string
	endpoints        []string
}

func (params Params) String() string {
	output := fmt.Sprintln("Test parameters")
	output += fmt.Sprintf("endpoint(s):      %s\n", params.endpoints)
	output += fmt.Sprintf("bucket:           %s\n", params.bucketName)
	output += fmt.Sprintf("objectNamePrefix: %s\n", params.objectNamePrefix)
	output += fmt.Sprintf("objectSize:       %0.4f MB\n", float64(params.objectSize)/(1024*1024))
	output += fmt.Sprintf("numClients:       %d\n", params.numClients)
	output += fmt.Sprintf("numSamples:       %d\n", params.numSamples)
	return output
}

// Contains the summary for a given test result
type Result struct {
	operation        string
	bytesTransmitted int64
	numErrors        int
	opDurations      []float64
	totalDuration    time.Duration
}

func (r Result) String() string {
	report := fmt.Sprintf("Results Summary for %s Operation(s)\n", r.operation)
	report += fmt.Sprintf("Total Transferred: %0.3f MB\n", float64(r.bytesTransmitted)/(1024*1024))
	report += fmt.Sprintf("Total Throughput:  %0.2f MB/s\n", (float64(r.bytesTransmitted)/(1024*1024))/r.totalDuration.Seconds())
	report += fmt.Sprintf("Total Duration:    %0.3f s\n", r.totalDuration.Seconds())
	report += fmt.Sprintf("Number of Errors:  %d\n", r.numErrors)
	if len(r.opDurations) > 0 {
		report += fmt.Sprintln("------------------------------------")
		report += fmt.Sprintf("Put times Max:       %0.3f s\n", r.percentile(100))
		report += fmt.Sprintf("Put times 99th %%ile: %0.3f s\n", r.percentile(99))
		report += fmt.Sprintf("Put times 90th %%ile: %0.3f s\n", r.percentile(90))
		report += fmt.Sprintf("Put times 75th %%ile: %0.3f s\n", r.percentile(75))
		report += fmt.Sprintf("Put times 50th %%ile: %0.3f s\n", r.percentile(50))
		report += fmt.Sprintf("Put times 25th %%ile: %0.3f s\n", r.percentile(25))
		report += fmt.Sprintf("Put times Min:       %0.3f s\n", r.percentile(0))
	}
	return report
}

func (r Result) percentile(i int) float64 {
	if i >= 100 {
		i = len(r.opDurations) - 1
	} else if i > 0 && i < 100 {
		i = int(float64(i) / 100 * float64(len(r.opDurations)))
	}
	return r.opDurations[i]
}

type Req interface{}

type Resp struct {
	err      error
	duration time.Duration
	numBytes int64
}