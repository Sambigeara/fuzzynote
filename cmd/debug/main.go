package main

func main() {
	// Copy walfile into `debug/` dir and run e.g.
	// go run cmd/debug/main.go "1323068878:1642754346644000000"

	//if len(os.Args) != 2 {
	//log.Print("provide list item key as a single argument")
	//os.Exit(0)
	//}

	////key := os.Args[1]

	//root := "debug/"
	//os.Mkdir(root, os.ModePerm)

	//webTokens := service.NewFileWebTokenStore(root)
	//localWalFile := service.NewLocalFileWalFile(root)
	//r := service.NewDBListRepo(localWalFile, webTokens)

	////r.DebugWriteEventsToFile(root, key)
	//c := make(chan []service.EventLog)
	//go func() {
	//for {
	//wal := <-c
	//if err := r.Replay(wal); err != nil {
	//return
	//}
	//}
	//}()
	//r.TestPullLocal(c)
	//matches, _, _ := r.Match([][]rune{}, true, "", 0, 0)
	//_ = matches
	//runtime.Breakpoint()
}
