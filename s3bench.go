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
	mathrand "math/rand"
	"encoding/json"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

const (
	opRead  = "Read"
	opWrite = "Write"
	opHeadObj = "HeadObj"
	opGetObjTag = "GetObjTag"
	opPutObjTag = "PutObjTag"
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
	headObj := flag.Bool("headObj", false, "head-object request instead of reading obj content")
	sampleReads := flag.Int("sampleReads", 1, "number of reads of each sample")
	clientDelay := flag.Int("clientDelay", 1, "delay in ms before client starts. if negative value provided delay will be randomized in interval [0, abs{clientDelay})")
	jsonOutput := flag.Bool("jsonOutput", false, "print results in forma of json")
	deleteAtOnce := flag.Int("deleteAtOnce", 1000, "number of objs to delete at once")
	putObjTag := flag.Bool("putObjTag", false, "put object's tags")
	getObjTag := flag.Bool("getObjTag", false, "get object's tags")
	numTags := flag.Int("numTags", 10, "number of tags to create, for objects it should in range [1..10]")

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

	if *deleteAtOnce < 1 {
		fmt.Println("Cannot delete less than 1 obj at once")
		os.Exit(1)
	}

	if *numTags < 1 {
		fmt.Println("-numTags cannot be less than 1")
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
		headObj:          *headObj,
		sampleReads:      uint(*sampleReads),
		clientDelay:      *clientDelay,
		jsonOutput:       *jsonOutput,
		deleteAtOnce:     *deleteAtOnce,
		putObjTag:        *putObjTag || *getObjTag,
		getObjTag:        *getObjTag,
		numTags:          uint(*numTags),
		readObj:          !(*putObjTag || *getObjTag || *headObj),
	}

	// Generate the data from which we will do the writting
	params.printf("Generating in-memory sample data...\n")
	timeGenData := time.Now()
	bufferBytes = make([]byte, params.objectSize, params.objectSize)
	_, err := rand.Read(bufferBytes)
	if err != nil {
		fmt.Printf("Could not allocate a buffer")
		os.Exit(1)
	}
	params.printf("Done (%s)\n", time.Since(timeGenData))

	// Start the load clients and run a write test followed by a read test
	cfg := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(*accessKey, *accessSecret, ""),
		Region:           aws.String(*region),
		S3ForcePathStyle: aws.Bool(true),
	}

	bucket_created := params.prepareBucket(cfg)

	params.StartClients(cfg)

	testResults := make([]Result, 2, 5)

	params.printf("Running %s test...\n", opWrite)
	testResults = append(testResults, params.Run(opWrite))

	if params.putObjTag {
		params.printf("Running %s test...\n", opPutObjTag)
		testResults = append(testResults, params.Run(opPutObjTag))
	}
	if params.getObjTag {
		params.printf("Running %s test...\n", opGetObjTag)
		testResults = append(testResults, params.Run(opGetObjTag))
	}
	if params.headObj {
		params.printf("Running %s test...\n", opHeadObj)
		testResults = append(testResults, params.Run(opHeadObj))
	}
	if params.readObj {
		params.printf("Running %s test...\n", opRead)
		testResults = append(testResults, params.Run(opRead))
	}

	// Do cleanup if required
	if !*skipCleanup {
		params.printf("Cleaning up %d objects...\n", *numSamples)
		delStartTime := time.Now()
		svc := s3.New(session.New(), cfg)

		numSuccessfullyDeleted := 0

		keyList := make([]*s3.ObjectIdentifier, 0, params.deleteAtOnce)
		for i := 0; i < *numSamples; i++ {
			objName := fmt.Sprintf("%s%d", *objectNamePrefix, i)
			key := aws.String(objName)

			if params.putObjTag {
				deleteObjectTaggingInput := &s3.DeleteObjectTaggingInput{
						Bucket: aws.String(*bucketName),
						Key:    key,
				}
				_, err := svc.DeleteObjectTagging(deleteObjectTaggingInput)
				params.printf("Delete tags %s |err %v\n", objName, err)
			}
			bar := s3.ObjectIdentifier{ Key: key, }
			keyList = append(keyList, &bar)
			if len(keyList) == params.deleteAtOnce || i == *numSamples-1 {
				params.printf("Deleting a batch of %d objects in range {%d, %d}... ", len(keyList), i-len(keyList)+1, i)
				dltpar := &s3.DeleteObjectsInput{
					Bucket: aws.String(*bucketName),
					Delete: &s3.Delete{
						Objects: keyList}}
				_, err := svc.DeleteObjects(dltpar)
				if err == nil {
					numSuccessfullyDeleted += len(keyList)
					params.printf("Succeeded\n")
				} else {
					params.printf("Failed (%v)\n", err)
				}
				//set cursor to 0 so we can move to the next batch.
				keyList = keyList[:0]

			}
		}
		params.printf("Successfully deleted %d/%d objects in %s\n", numSuccessfullyDeleted, *numSamples, time.Since(delStartTime))

		if bucket_created {
			params.printf("Deleting bucket...\n")
			dltpar := &s3.DeleteBucketInput{
				Bucket: aws.String(*bucketName)}
			_, err := svc.DeleteBucket(dltpar)
			if err == nil {
				params.printf("Succeeded\n")
			} else {
				params.printf("Failed (%v)\n", err)
			}
		}
	}

	params.reportPrint(params.reportPrepare(testResults))
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
		if resp.err != nil {
			errStr := fmt.Sprintf("%v(%d) completed in %0.2fs with error %s",
				op, i+1, resp.duration.Seconds(), resp.err)
			result.opErrors = append(result.opErrors, errStr)
		} else {
			result.bytesTransmitted = result.bytesTransmitted + params.objectSize
			result.opDurations = append(result.opDurations, resp.duration.Seconds())
			result.opTtfb = append(result.opTtfb, resp.ttfb.Seconds())
		}
		params.printf("operation %s(%d) completed in %.2fs|%s\n", op, i+1, resp.duration.Seconds(), resp.err)
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
		} else if op == opPutObjTag {
			tagSet := make([]*s3.Tag, 0, params.numTags)
			for iTag := uint(0); iTag < params.numTags; iTag++ {
				key   := fmt.Sprintf("%s%d", "key_",   iTag);
				value := fmt.Sprintf("%s%d", "value_", iTag)
				tagSet = append(tagSet, &s3.Tag {
						Key:   &key,
						Value: &value,
						})
			}
			params.requests <- &s3.PutObjectTaggingInput{
				Bucket: bucket,
				Key:    key,
				Tagging: &s3.Tagging{
						TagSet: tagSet,
				},
			}
		} else if op == opGetObjTag {
			params.requests <- &s3.GetObjectTaggingInput{
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
		if params.clientDelay > 0 {
			time.Sleep(time.Duration(params.clientDelay) *
				time.Millisecond)
		} else if params.clientDelay < 0 {
			time.Sleep(time.Duration(mathrand.Intn(-params.clientDelay)) *
				time.Millisecond)
		}
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
		case *s3.PutObjectTaggingInput:
			req, _ := svc.PutObjectTaggingRequest(r)
			err = req.Send()
			ttfb = time.Since(putStartTime)
		case *s3.GetObjectTaggingInput:
			req, _ := svc.GetObjectTaggingRequest(r)
			err = req.Send()
			ttfb = time.Since(putStartTime)
		default:
			panic("Developer error")
		}

		params.responses <- Resp{err, time.Since(putStartTime), numBytes, ttfb}
	}
}

// Specifies the parameters for a given test
type Params struct {
	requests         chan Req
	responses        chan Resp
	numSamples       uint
	numClients       uint
	objectSize       int64
	objectNamePrefix string
	bucketName       string
	endpoints        []string
	verbose          bool
	headObj          bool
	sampleReads      uint
	clientDelay      int
	jsonOutput       bool
	deleteAtOnce     int
	putObjTag        bool
	getObjTag        bool
	numTags          uint
	readObj          bool
}

func (params Params) printf(f string, args ...interface{}) {
	if params.verbose {
		fmt.Printf(f, args...)
	}
}

// Contains the summary for a given test result
type Result struct {
	operation        string
	bytesTransmitted int64
	opDurations      []float64
	totalDuration    time.Duration
	opTtfb           []float64
	opErrors         []string
}

func (r Result) report() map[string]interface{} {
	ret := make(map[string]interface{})
	ret["Operation"] = r.operation
	ret["Total Requests Count"] = len(r.opDurations)
	if r.operation != opHeadObj {
		ret["Total Transferred (MB)"] = float64(r.bytesTransmitted)/(1024*1024)
		ret["Total Throughput (MB/s)"] = (float64(r.bytesTransmitted)/(1024*1024))/r.totalDuration.Seconds()
	}
	ret["Total Duration (s)"] = r.totalDuration.Seconds()

	if len(r.opDurations) > 0 {
		ret["Duration Max"] = percentile(r.opDurations, 100)
		ret["Duration Avg"] = avg(r.opDurations)
		ret["Duration Min"] = percentile(r.opDurations, 0)
		ret["Duration 99th-ile"] = percentile(r.opDurations, 99)
		ret["Duration 90th-ile"] = percentile(r.opDurations, 90)
		ret["Duration 75th-ile"] = percentile(r.opDurations, 75)
		ret["Duration 50th-ile"] = percentile(r.opDurations, 50)
		ret["Duration 25th-ile"] = percentile(r.opDurations, 25)
	}

	if len(r.opTtfb) > 0 {
		ret["Ttfb Max"] = percentile(r.opTtfb, 100)
		ret["Ttfb Avg"] = avg(r.opTtfb)
		ret["Ttfb Min"] = percentile(r.opTtfb, 0)
		ret["Ttfb 99th-ile"] = percentile(r.opTtfb, 99)
		ret["Ttfb 90th-ile"] = percentile(r.opTtfb, 90)
		ret["Ttfb 75th-ile"] = percentile(r.opTtfb, 75)
		ret["Ttfb 50th-ile"] = percentile(r.opTtfb, 50)
		ret["Ttfb 25th-ile"] = percentile(r.opTtfb, 25)
	}

	ret["Errors Count"] = len(r.opErrors)
	ret["Errors"] = r.opErrors
	return ret
}

func (params Params) report() map[string]interface{} {
	ret := make(map[string]interface{})
	ret["endpoints"] =  params.endpoints
	ret["bucket"] = params.bucketName
	ret["objectNamePrefix"] = params.objectNamePrefix
	ret["objectSize (MB)"] = float64(params.objectSize)/(1024*1024)
	ret["numClients"] = params.numClients
	ret["numSamples"] = params.numSamples
	ret["sampleReads"] = params.sampleReads
	ret["verbose"] = params.verbose
	ret["headObj"] = params.headObj
	ret["clientDelay"] = params.clientDelay
	ret["jsonOutput"] = params.jsonOutput
	ret["deleteAtOnce"] = params.deleteAtOnce
	ret["numTags"] = params.numTags
	ret["putObjTag"] = params.putObjTag
	ret["getObjTag"] = params.getObjTag
	ret["readObj"] = params.readObj
	return ret
}

func (params Params) reportPrepare(tests []Result) map[string]interface{} {
	report := make(map[string]interface{})
	report["Parameters"] = params.report()
	testreps := make([]map[string]interface{}, 0, len(tests))
	for _, r := range tests {
		testreps = append(testreps, r.report())
	}
	report["Tests"] = testreps
	return report
}

func mapPrint(m map[string]interface{}, prefix string) {
	for k,v := range m {
		fmt.Printf("%s %-27s", prefix, k+":")
		switch val := v.(type) {
		case []string:
			if len(val) == 0 {
				fmt.Printf(" []\n")
			} else {
				fmt.Println()
				for _, s := range val {
					fmt.Printf("%s%s %s\n", prefix, prefix, s)
				}
			}
		case map[string]interface{}:
			fmt.Println()
			mapPrint(val, prefix + "   ")
		case []map[string]interface{}:
			if len(val) == 0 {
				fmt.Printf(" []\n")
			} else {
				for _, m := range val {
					fmt.Println()
					mapPrint(m, prefix + "   ")
				}
			}
		case float64:
			fmt.Printf(" %.3f\n", val)
		default:
			fmt.Printf(" %v\n", val)
		}
	}
}

func (params Params) reportPrint(report map[string]interface{}) {
	if params.jsonOutput {
		b, err := json.Marshal(report)
		if err != nil {
			fmt.Println("Cannot generate JSON report %v", err)
		}
		fmt.Println(string(b))
		return
	}

	mapPrint(report, "")
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
