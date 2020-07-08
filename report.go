package main

import (
	"fmt"
	"sort"
	"strings"
	"encoding/json"
)

func keysSort(keys []string, format []string) []string {
	sort.Strings(keys)
	cur_formated := 0

	for _, fv := range format {
		fv = strings.TrimSpace(fv)
		should_del := strings.HasPrefix(fv, "-")
		if should_del {
			fv = fv[1:]
		}
		ci := indexOf(keys, fv)
		if ci < 0 {
			continue
		}
		// delete old pos
		keys = append(keys[:ci], keys[ci+1:]...)

		if !should_del {
			// insert new pos
			keys = append(keys[:cur_formated], append([]string{fv}, keys[cur_formated:]...)...)
			cur_formated++
		}
	}

	return keys
}

func formatFilter(format []string, key string) []string {
	ret := []string{}
	for _, v := range format {
		if strings.HasPrefix(v, key + ":") {
			ret = append(ret, v[len(key + ":"):])
		} else if strings.HasPrefix(v, "-" + key + ":") {
			ret = append(ret, "-" + v[len("-" + key + ":"):])
		}
	}

	return ret
}

func mapPrint(m map[string]interface{}, repFormat []string, prefix string) {
	var mkeys []string
	for k,_ := range m {
		mkeys = append(mkeys, k)
	}
	mkeys = keysSort(mkeys, repFormat)
	for _, k := range mkeys {
		v := m[k]
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
			mapPrint(val, formatFilter(repFormat, k), prefix + "   ")
		case []map[string]interface{}:
			if len(val) == 0 {
				fmt.Printf(" []\n")
			} else {
				val_format := formatFilter(repFormat, k)
				for _, m := range val {
					fmt.Println()
					mapPrint(m, val_format, prefix + "   ")
				}
			}
		case float64:
			fmt.Printf(" %.3f\n", val)
		default:
			fmt.Printf(" %v\n", val)
		}
	}
}

func (params Params) reportPrepare(tests []Result) map[string]interface{} {
	report := make(map[string]interface{})
	report["Version"] = fmt.Sprintf("%s-%s", buildDate, gitHash)
	report["Parameters"] = params.report()
	testreps := make([]map[string]interface{}, 0, len(tests))
	for _, r := range tests {
		testreps = append(testreps, r.report())
	}
	report["Tests"] = testreps
	return report
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

	mapPrint(report, strings.Split(params.reportFormat, ";"), "")
}

func (r Result) report() map[string]interface{} {
	ret := make(map[string]interface{})
	ret["Operation"] = r.operation
	ret["Total Requests Count"] = len(r.opDurations)
	if r.operation == opWrite || r.operation == opRead || r.operation == opValidate {
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
	ret["tagNamePrefix"] = params.tagNamePrefix
	ret["tagValPrefix"] = params.tagValPrefix
	ret["reportFormat"] = params.reportFormat
	ret["validate"] = params.validate
	ret["skipWrite"] = params.skipWrite
	ret["skipRead"] = params.skipRead
	return ret
}
