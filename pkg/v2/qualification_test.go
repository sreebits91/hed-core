package v2

import("context";"encoding/json";"fmt";"os";"sort";"strconv";"testing";"time")

func percentile(samples []int64,p float64)time.Duration{if len(samples)==0{return 0};sort.Slice(samples,func(i,j int)bool{return samples[i]<samples[j]});idx:=int(float64(len(samples)-1)*p);return time.Duration(samples[idx])}

func TestQualification(t *testing.T){
	if os.Getenv("HED_QUALIFY")!="1"{t.Skip("set HED_QUALIFY=1 to run load qualification")}
	n:=500000;if s:=os.Getenv("HED_LOAD_N");s!=""{v,e:=strconv.Atoi(s);if e!=nil||v<=0{t.Fatal(e)};n=v}
	c:=DefaultConfig();c.Partitions=32;c.QueueCapacity=65536;c.BatchSize=512;c.FlushInterval=time.Millisecond;c.WALPath="";p,e:=NewPipeline(c,discardBackend{});if e!=nil{t.Fatal(e)}
	start:=time.Now();accepted:=0;const reservoir=20000;samples:=make([]int64,0,reservoir);var maxDepth int
	for accepted<n{t0:=time.Now();_,e=p.Submit(context.Background(),Tx{ID:"qual-"+itoa(accepted),Key:"account-"+itoa(accepted%100000),Payload:[]byte("qualification")});if e==ErrQueueFull{time.Sleep(time.Microsecond);continue};if e!=nil{p.Stop();t.Fatal(e)};accepted++;d:=time.Since(t0).Nanoseconds();if len(samples)<reservoir{samples=append(samples,d)}else{samples[accepted%reservoir]=d};for i:=range p.parts{if depth:=p.parts[i].q.Len();depth>maxDepth{maxDepth=depth}}}
	elapsed:=time.Since(start);p.Stop();tps:=float64(accepted)/elapsed.Seconds();m:=p.Metrics().Snapshot();result:=map[string]any{"load":accepted,"elapsed":elapsed.String(),"tps":tps,"submit_p50":percentile(samples,.50).String(),"submit_p95":percentile(samples,.95).String(),"submit_p99":percentile(samples,.99).String(),"max_queue_depth":maxDepth,"metrics":m};b,_:=json.Marshal(result);fmt.Println(string(b))
}
