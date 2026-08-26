package v2

import("context";"fmt";"os";"strconv";"testing";"time")

func TestQualification(t *testing.T){if os.Getenv("HED_QUALIFY")!="1"{t.Skip("set HED_QUALIFY=1 to run load qualification")};n:=500000;if s:=os.Getenv("HED_LOAD_N");s!=""{v,e:=strconv.Atoi(s);if e!=nil||v<=0{t.Fatal(e)};n=v};c:=DefaultConfig();c.Partitions=32;c.QueueCapacity=65536;c.BatchSize=512;c.FlushInterval=time.Millisecond;c.WALPath="";p,e:=NewPipeline(c,discardBackend{});if e!=nil{t.Fatal(e)};start:=time.Now();accepted:=0;for accepted<n{_,e:=p.Submit(context.Background(),Tx{ID:"qual-"+itoa(accepted),Key:"account-"+itoa(accepted%100000),Payload:[]byte("qualification")});if e==ErrQueueFull{time.Sleep(time.Microsecond);continue};if e!=nil{p.Stop();t.Fatal(e)};accepted++};p.Stop();elapsed:=time.Since(start);tps:=float64(accepted)/elapsed.Seconds();fmt.Printf("HED qualification load=%d elapsed=%s TPS=%.2f\n",accepted,elapsed,tps)}
