package main

import (
	"fmt"
	"strconv"
	"regexp"
	"encoding/base32"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"

)

func to_b32(dt []byte) string {
	return base32.StdEncoding.WithPadding(base32.NoPadding).EncodeToString(dt)
}

func from_b32(s string) ([]byte, error) {
	return base32.StdEncoding.WithPadding(base32.NoPadding).DecodeString(s)
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
		panic("Invalid objectSize value format\n")
	}
	val, err := strconv.ParseInt(string(mm[1]), 10, 64)
	mult, ex := sizes[string(mm[2])]
	if !ex || err != nil {
		panic("Invalid objectSize value\n")
	}
	return val * mult
}

func (params Params) printf(f string, args ...interface{}) {
	if params.verbose {
		fmt.Printf(f, args...)
	}
}

// samples per operation
func (params Params) spo(op string) uint {
	if op == opWrite || op == opPutObjTag || op == opValidate {
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

func indexOf(sls []string, s string) int {
	ret := -1
	for i, v := range sls {
		if v == s {
			ret = i
			break
		}
	}
	return ret
}

func genObjName(pref string, hsh string, idx uint) *string {
	return aws.String(fmt.Sprintf("%s_%s_%d", pref, hsh, idx))
}

func (params *Params) getObjectHash(cfg *aws.Config) (string, error){
	cfg.Endpoint = aws.String(params.endpoints[0])
	svc := s3.New(session.New(), cfg)

	result, err := svc.ListObjectsV2(&s3.ListObjectsV2Input{
		Bucket: aws.String(params.bucketName),
		MaxKeys: aws.Int64(1),
		Prefix: aws.String(params.objectNamePrefix),
	})

	if err != nil {
		return "", err
	}
	if len(result.Contents) == 0 {
		return "", fmt.Errorf("Empty bucket")
	}

	re := regexp.MustCompile(`^.*_([A-Z2-7]+)_[0-9]+$`)
	mm := re.FindStringSubmatch(*result.Contents[0].Key)
	if len(mm) != 2 {
		return "", fmt.Errorf("Invalid object name format")
	}

	return mm[1], nil
}
