# kafkabulkcommit

simple kafka commit to simulate bulk commit using confluence kafka

u can simulate produce and consume message with this code.

how to run:

producer :
```go
go run main.go -type=producer -publishCount=<number of message to publish>
```

consumer :
```go
go run main.go -type=consumer -worker=<number of worker to do concurrent processing>
```
