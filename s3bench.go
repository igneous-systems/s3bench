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
	"regexp"
	"strconv"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	opRead  = "Read"
	opWrite = "Write"
	opHeadObj = "HeadObj"
	//max that can be deleted at a time via DeleteObjects()
	commitSize = 1000
)

var bufferBytes []byte

// true if created
// false if existed
func (params *Params) prepareBucket(cfg *aws.Config) bool {
	cfg.Endpoint = aws.String(params.endpoints[0])
	svc := s3.New(session.New(), cfg)
	req, _ := svc.CreateBucketRequest(
		&s3.CreateBucketInput{Bucket: aws.String(params.bucketName)})

	err := req.Send()

	if err == nil {
		return true
	} else if !strings.Contains(err.Error(), "BucketAlreadyOwnedByYou:") &&
		!strings.Contains(err.Error(), "BucketAlreadyExists:") {
		panic("Failed to create bucket: " + err.Error())
	}

	return false
}

func parse_size(sz string) int64 {
	sizes := map[string]int64 {
		"b": 1,
		"Kb": 1024,
		"Mb": 1024 * 1024,
		"Gb": 1024 * 1024 * 1024,
	}
	re := regexp.MustCompile(`^(\d+)([bKMG]{1,2})$`)
	mm := re.FindStringSubmatch(sz)
	if len(mm) != 3 {
		fmt.Printf("Invalid objectSize value format\n")
		os.Exit(1)
	}
	val, err := strconv.ParseInt(string(mm[1]), 10, 64)
	mult, ex := sizes[string(mm[2])]
	if !ex || err != nil {
		fmt.Printf("Invalid objectSize value\n")
		os.Exit(1)
	}
	return val * mult
}

func main() {
	endpoint := flag.String("endpoint", "", "S3 endpoint(s) comma separated - http://IP:PORT,http://IP:PORT")
	region := flag.String("region", "igneous-test", "AWS region to use, eg: us-west-1|us-east-1, etc")
	accessKey := flag.String("accessKey", "", "the S3 access key")
	accessSecret := flag.String("accessSecret", "", "the S3 access secret")
	bucketName := flag.String("bucket", "bucketname", "the bucket for which to run the test")
	objectNamePrefix := flag.String("objectNamePrefix", "loadgen_test_", "prefix of the object name that will be used")
	objectSize := flag.String("objectSize", "80Mb", "size of individual requests (must be smaller than main memory)")
	numClients := flag.Int("numClients", 40, "number of concurrent clients")
	numSamples := flag.Int("numSamples", 200, "total number of requests to send")
	skipCleanup := flag.Bool("skipCleanup", false, "skip deleting objects created by this tool at the end of the run")
	verbose := flag.Bool("verbose", false, "print verbose per thread status")
	metaData := flag.Bool("metaData", false, "read obj metadata instead of obj itself")
	sampleReads := flag.Int("sampleReads", 1, "number of reads of each sample")

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
		numSamples:       uint(*numSamples),
		numClients:       uint(*numClients),
		objectSize:       parse_size(*objectSize),
		objectNamePrefix: *objectNamePrefix,
		bucketName:       *bucketName,
		endpoints:        strings.Split(*endpoint, ","),
		verbose:          *verbose,
		metaData:         *metaData,
		sampleReads:      uint(*sampleReads),
	}
	fmt.Println(params)
	fmt.Println()

	// Generate the data from which we will do the writting
	fmt.Printf("Generating in-memory sample data... ")
	timeGenData := time.Now()
	bufferBytes = make([]byte, params.objectSize, params.objectSize)
	_, err := rand.Read(bufferBytes)
	if err != nil {
		fmt.Printf("Could not allocate a buffer")
		os.Exit(1)
	}
	fmt.Printf("Done (%s)\n", time.Since(timeGenData))
	fmt.Println()

	// Start the load clients and run a write test followed by a read test
	cfg := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(*accessKey, *accessSecret, ""),
		Region:           aws.String(*region),
		S3ForcePathStyle: aws.Bool(true),
	}

	bucket_created := params.prepareBucket(cfg)

	params.StartClients(cfg)

	fmt.Printf("Running %s test...\n", opWrite)
	writeResult := params.Run(opWrite)
	fmt.Println()

	var readResult = Result{}
	if params.metaData {
		fmt.Printf("Running %s test...\n", opHeadObj)
		readResult = params.Run(opHeadObj)
		fmt.Println()
	} else {
		fmt.Printf("Running %s test...\n", opRead)
		readResult = params.Run(opRead)
		fmt.Println()
	}

	// Repeating the parameters of the test followed by the results
	fmt.Println(params)
	fmt.Println()
	fmt.Println(writeResult)
	fmt.Println()
	fmt.Println(readResult)

	// Do cleanup if required
	if !*skipCleanup {
		fmt.Println()
		fmt.Printf("Cleaning up %d objects...\n", *numSamples)
		delStartTime := time.Now()
		svc := s3.New(session.New(), cfg)

		numSuccessfullyDeleted := 0

		keyList := make([]*s3.ObjectIdentifier, 0, commitSize)
		for i := 0; i < *numSamples; i++ {
			bar := s3.ObjectIdentifier{
				Key: aws.String(fmt.Sprintf("%s%d", *objectNamePrefix, i)),
			}
			keyList = append(keyList, &bar)
			if len(keyList) == commitSize || i == *numSamples-1 {
				fmt.Printf("Deleting a batch of %d objects in range {%d, %d}... ", len(keyList), i-len(keyList)+1, i)
				params := &s3.DeleteObjectsInput{
					Bucket: aws.String(*bucketName),
					Delete: &s3.Delete{
						Objects: keyList}}
				_, err := svc.DeleteObjects(params)
				if err == nil {
					numSuccessfullyDeleted += len(keyList)
					fmt.Printf("Succeeded\n")
				} else {
					fmt.Printf("Failed (%v)\n", err)
				}
				//set cursor to 0 so we can move to the next batch.
				keyList = keyList[:0]

			}
		}
		fmt.Printf("Successfully deleted %d/%d objects in %s\n", numSuccessfullyDeleted, *numSamples, time.Since(delStartTime))

		if bucket_created {
			fmt.Printf("Deleting bucket...\n")
			params := &s3.DeleteBucketInput{
				Bucket: aws.String(*bucketName)}
			_, err := svc.DeleteBucket(params)
			if err == nil {
				fmt.Printf("Succeeded\n")
			} else {
				fmt.Printf("Failed (%v)\n", err)
			}
		}
	}
}

func (params *Params) Run(op string) Result {
	startTime := time.Now()

	// Start submitting load requests
	go params.submitLoad(op)

	opSamples := params.spo(op)
	// Collect and aggregate stats for completed requests
	result := Result{opDurations: make([]float64, 0, opSamples), operation: op}
	for i := uint(0); i < opSamples; i++ {
		resp := <-params.responses
		errorString := ""
		if resp.err != nil {
			result.numErrors++
			errorString = fmt.Sprintf(", error: %s", resp.err)
		} else {
			result.bytesTransmitted = result.bytesTransmitted + params.objectSize
			result.opDurations = append(result.opDurations, resp.duration.Seconds())
			result.opTtfb = append(result.opTtfb, resp.ttfb.Seconds())
		}
		if params.verbose {
			fmt.Printf("%v operation completed in %0.2fs (%d/%d) - %0.2fMB/s%s\n",
				op, resp.duration.Seconds(), i+1, params.numSamples,
				(float64(result.bytesTransmitted)/(1024*1024))/time.Since(startTime).Seconds(),
				errorString)
		}
	}

	result.totalDuration = time.Since(startTime)
	sort.Float64s(result.opDurations)
	sort.Float64s(result.opTtfb)
	return result
}

// Create an individual load request and submit it to the client queue
func (params *Params) submitLoad(op string) {
	bucket := aws.String(params.bucketName)
	opSamples := params.spo(op)
	for i := uint(0); i < opSamples; i++ {
		key := aws.String(fmt.Sprintf("%s%d", params.objectNamePrefix, i % params.numSamples))
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
		} else if op == opHeadObj {
			params.requests <- &s3.HeadObjectInput{
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
		time.Sleep(1 * time.Millisecond)
	}
}

// Run an individual load request
func (params *Params) startClient(cfg *aws.Config) {
	svc := s3.New(session.New(), cfg)
	for request := range params.requests {
		putStartTime := time.Now()
		var ttfb time.Duration
		var err error
		numBytes := params.objectSize

		switch r := request.(type) {
		case *s3.PutObjectInput:
			req, _ := svc.PutObjectRequest(r)
			// Disable payload checksum calculation (very expensive)
			req.HTTPRequest.Header.Add("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
			err = req.Send()
			ttfb = time.Since(putStartTime)
		case *s3.GetObjectInput:
			req, resp := svc.GetObjectRequest(r)
			err = req.Send()
			ttfb = time.Since(putStartTime)
			numBytes = 0
			if err == nil {
				numBytes, err = io.Copy(ioutil.Discard, resp.Body)
			}
			if numBytes != params.objectSize {
				err = fmt.Errorf("expected object length %d, actual %d", params.objectSize, numBytes)
			}
		case *s3.HeadObjectInput:
			req, resp := svc.HeadObjectRequest(r)
			err = req.Send()
			ttfb = time.Since(putStartTime)
			numBytes = 0
			if err == nil {
				numBytes = *resp.ContentLength
			}
			if numBytes != params.objectSize {
				err = fmt.Errorf("expected object length %d, actual %d, resp %v", params.objectSize, numBytes, resp)
			}
		default:
			panic("Developer error")
		}

		params.responses <- Resp{err, time.Since(putStartTime), numBytes, ttfb}
	}
}

// Specifies the parameters for a given test
type Params struct {
	operation        string
	requests         chan Req
	responses        chan Resp
	numSamples       uint
	numClients       uint
	objectSize       int64
	objectNamePrefix string
	bucketName       string
	endpoints        []string
	verbose          bool
	metaData         bool
	sampleReads      uint
}

func (params Params) String() string {
	output := fmt.Sprintln("Test parameters")
	output += fmt.Sprintf("endpoint(s):      %s\n", params.endpoints)
	output += fmt.Sprintf("bucket:           %s\n", params.bucketName)
	output += fmt.Sprintf("objectNamePrefix: %s\n", params.objectNamePrefix)
	output += fmt.Sprintf("objectSize:       %0.4f MB\n", float64(params.objectSize)/(1024*1024))
	output += fmt.Sprintf("numClients:       %d\n", params.numClients)
	output += fmt.Sprintf("numSamples:       %d\n", params.numSamples)
	output += fmt.Sprintf("sampleReads:      %d\n", params.sampleReads)
	output += fmt.Sprintf("verbose:       %d\n", params.verbose)
	output += fmt.Sprintf("metaData:      %d\n", params.metaData)
	return output
}

// Contains the summary for a given test result
type Result struct {
	operation        string
	bytesTransmitted int64
	numErrors        int
	opDurations      []float64
	totalDuration    time.Duration
	opTtfb           []float64
}

func (r Result) String() string {
	report := fmt.Sprintf("Results Summary for %s Operation(s)\n", r.operation)
	if r.operation == opHeadObj {
		report += fmt.Sprintf("Total Reqs: %d\n", len(r.opDurations))
	} else {
		report += fmt.Sprintf("Total Transferred: %0.3f MB\n", float64(r.bytesTransmitted)/(1024*1024))
		report += fmt.Sprintf("Total Throughput:  %0.2f MB/s\n", (float64(r.bytesTransmitted)/(1024*1024))/r.totalDuration.Seconds())
	}
	report += fmt.Sprintf("Total Duration:    %0.3f s\n", r.totalDuration.Seconds())
	report += fmt.Sprintf("Number of Errors:  %d\n", r.numErrors)

	if len(r.opDurations) > 0 {
		report += fmt.Sprintln("------------------------------------")
		report += fmt.Sprintf("%s times Avg:       %0.3f s\n", r.operation, avg(r.opDurations))
		report += fmt.Sprintf("%s times Max:       %0.3f s\n", r.operation, percentile(r.opDurations, 100))
		report += fmt.Sprintf("%s times Min:       %0.3f s\n", r.operation, percentile(r.opDurations, 0))
		report += fmt.Sprintf("%s times 99th %%ile: %0.3f s\n", r.operation, percentile(r.opDurations, 99))
		report += fmt.Sprintf("%s times 90th %%ile: %0.3f s\n", r.operation, percentile(r.opDurations, 90))
		report += fmt.Sprintf("%s times 75th %%ile: %0.3f s\n", r.operation, percentile(r.opDurations, 75))
		report += fmt.Sprintf("%s times 50th %%ile: %0.3f s\n", r.operation, percentile(r.opDurations, 50))
		report += fmt.Sprintf("%s times 25th %%ile: %0.3f s\n", r.operation, percentile(r.opDurations, 25))
	}

	if len(r.opTtfb) > 0 {
		report += fmt.Sprintln("------------------------------------")
		report += fmt.Sprintf("%s ttfb Avg:       %0.3f s\n", r.operation, avg(r.opTtfb))
		report += fmt.Sprintf("%s ttfb Max:       %0.3f s\n", r.operation, percentile(r.opTtfb, 100))
		report += fmt.Sprintf("%s ttfb Min:       %0.3f s\n", r.operation, percentile(r.opTtfb, 0))
		report += fmt.Sprintf("%s ttfb 99th %%ile: %0.3f s\n", r.operation, percentile(r.opTtfb, 99))
		report += fmt.Sprintf("%s ttfb 90th %%ile: %0.3f s\n", r.operation, percentile(r.opTtfb, 90))
		report += fmt.Sprintf("%s ttfb 75th %%ile: %0.3f s\n", r.operation, percentile(r.opTtfb, 75))
		report += fmt.Sprintf("%s ttfb 50th %%ile: %0.3f s\n", r.operation, percentile(r.opTtfb, 50))
		report += fmt.Sprintf("%s ttfb 25th %%ile: %0.3f s\n", r.operation, percentile(r.opTtfb, 25))
	}
	return report
}

// samples per operation
func (params Params) spo(op string) uint {
	if op == opWrite {
		return params.numSamples
	}

	return params.numSamples * params.sampleReads
}

func percentile(dt []float64, i int) float64 {
	ln := len(dt)
	if i >= 100 {
		i = ln - 1
	} else if i > 0 && i < 100 {
		i = int(float64(i) / 100 * float64(ln))
	}
	return dt[i]
}

func avg(dt []float64) float64 {
	ln := float64(len(dt))
	sm := float64(0)
	for _, el := range dt {
		sm += el
	}
	return sm / ln
}

type Req interface{}

type Resp struct {
	err      error
	duration time.Duration
	numBytes int64
	ttfb     time.Duration
}
