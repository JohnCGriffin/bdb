/*********************************************************************

 bdb - big disk branches on Linux/Mac

 Report back list of large directories with size larger than 4G.

 Note that this purposely does not cross file systems, so using
 'bdb /' will report on the root file system, not everything under its
 directory structure.  Likewise, bdb purposely avoids symlinks.

 options:
    -threads N  (number of threads, default 4)
    -size N (minimum GB of interest, default 1)

**********************************************************************/

package main

import "fmt"
import "log"
import "os"
import "syscall"
import "strings"
import "flag"
import "sort"

const oneGB = (1024 * 1024 * 1024)

func deviceNumber(dir string) (uint64, bool) {
	stat, err := os.Lstat(dir)
	if err != nil {
		return 0, false
	}
	return uint64(stat.Sys().(*syscall.Stat_t).Dev), true
}

func lStats(dir string, device uint64) []os.FileInfo {
	result := make([]os.FileInfo, 0)
	if dt, ok := deviceNumber(dir); !ok || dt != device {
		return result
	}
	f, err := os.Open(dir)
	if err != nil {
		return result
	}
	defer f.Close()
	names, err := f.Readdirnames(0)
	if err != nil {
		return result
	}
	for _, name := range names {
		stat, err := os.Lstat(dir + "/" + name)
		if err != nil {
			continue
		}
		mode := stat.Mode()
		if (mode & os.ModeSymlink) != 0 {
			continue
		}
		if mode.IsDir() || mode.IsRegular() {
			result = append(result, stat)
		}
	}
	return result
}

type summaryNode struct {
	size     uint64
	fullpath string
	children []summaryNode
}

func (node *summaryNode) appendChildNode(child summaryNode) {
	node.size += child.size
	if oneGB < child.size {
		node.children = append(node.children, child)
	}
}

func (node *summaryNode) dump(minSize uint64, collapsed bool) {
	sort.Slice(node.children, func(i, j int) bool { return node.children[i].size > node.children[j].size })
	if node.size >= minSize {
		size := float64(node.size)
		fmt.Printf("%s %.1f\n", node.fullpath, (size / oneGB))
		if collapsed && len(node.children) == 1 && node.children[0].size >= minSize {
			for len(node.children) == 1 && node.children[0].size >= minSize {
				node = &node.children[0]
			}
			node.dump(minSize, collapsed)
		} else {
			for _, child := range node.children {
				child.dump(minSize, collapsed)
			}
		}
	}
}

func worker(dir string, device uint64) summaryNode {

	result := summaryNode{fullpath: dir}

	for _, stat := range lStats(dir, device) {
		mode := stat.Mode()
		if mode.IsDir() {
			fullpath := strings.ReplaceAll(dir+"/"+stat.Name(), "//", "/")
			result.appendChildNode(worker(fullpath, device))
		} else if mode.IsRegular() {
			blocks := uint64(stat.Sys().(*syscall.Stat_t).Blocks)
			result.size += uint64(512 * blocks)
		}
	}

	return result
}

func workerThread(device uint64, dirJobs <-chan string, summary chan<- summaryNode, completed chan<- bool) {
	for dir := range dirJobs {
		child := worker(dir, device)
		if child.size > 0 {
			summary <- child
		}
	}
	completed <- true
}

func main() {

	threadsPtr := flag.Int("threads", 4, "number of threads (4 appropriate for SSD, 1 for magnetic disk)")
	sizePtr := flag.Uint64("size", 1, "minimum reportable size in GBs")
	noElisionPtr := flag.Bool("no-elision", false, "full display of repetitive directory traversal")

	flag.Parse()

	if len(flag.Args()) == 0 {
		flag.Usage()
		return
	}

	dir := flag.Arg(0)
	dev, ok := deviceNumber(dir)
	if !ok {
		log.Fatal("failed: deviceNumber(" + dir + ")")
	}

	threads := *threadsPtr
	minimumReportableSize := *sizePtr * oneGB
	collapsed := !(*noElisionPtr)

	dirJobs := make(chan string, threads)
	summary := make(chan summaryNode, threads)
	completed := make(chan bool)

	for i := 0; i < threads; i++ {
		go workerThread(dev, dirJobs, summary, completed)
	}

	dirs := make([]string, 0)

	result := summaryNode{fullpath: dir}

	for _, stat := range lStats(dir, dev) {

		if stat.Mode().IsRegular() {
			result.size += uint64(stat.Size())

		} else if stat.Mode().IsDir() {
			fullpath := strings.ReplaceAll(dir+"/"+stat.Name(), "//", "/")
			dirs = append(dirs, fullpath)
		}
	}

	go func() {
		for _, dir := range dirs {
			dirJobs <- dir
		}
		close(dirJobs)
	}()

	for threads > 0 {
		select {
		case <-completed:
			threads--
		case child := <-summary:
			result.appendChildNode(child)
		}
	}


	result.dump(minimumReportableSize, collapsed)
}
