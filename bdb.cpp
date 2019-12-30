
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

#include <cstdio>
#include <vector>
#include <exception>
#include <iomanip>
#include <future>
#include <mutex>
#include <queue>
#include <cstring>
#include <algorithm>

#include <sys/stat.h>
#include <dirent.h>


const size_t GB = 1024 * 1024 * 1024;

struct Node;
using NodePtr = std::shared_ptr<Node>;

struct Node {
    std::string fullpath;
    size_t size;
    std::vector<NodePtr> children;
};

static NodePtr
traverse_directory(const std::string dir,
		   const dev_t device,
		   const std::function<NodePtr(std::string,dev_t)> f)
{
    auto result = std::make_shared<Node>();
    result->fullpath = dir;

    auto dirp = opendir(dir.c_str());

    if(dirp){

	dirent *entry;
	
	while((entry = readdir(dirp)) != 0){

	    std::string name = entry->d_name;

	    if(name == "" || name == "." || name == ".."){
		continue;
	    }
	
	    const auto path = dir + (dir.back() == '/' ? "" : "/") + name;

	    struct stat buf;
	    if(lstat(path.c_str(), &buf) || buf.st_dev != device){
		continue;
	    }

	    if(entry->d_type == DT_DIR){

	        auto child = f(path,device);
		result->size += child->size;

		if (child->size >= GB) {
		    result->children.push_back(child);
		}

	    } else if(entry->d_type == DT_REG){
	
		result->size += buf.st_blocks * 512; // man 2 stat
	    }

	}
	
	closedir(dirp);
    }

    return result;
}

static NodePtr disk_consumption(std::string dir, const dev_t device)
{
    return traverse_directory(dir, device, disk_consumption);
}

static bool remove(std::queue<std::string>* q, std::string& receiver)
{
    static std::mutex m;
    std::lock_guard<std::mutex> guard(m);
    if(q->empty()){
	return false;
    }
    receiver = q->front();
    q->pop();
    return true;
}

static NodePtr worker(std::queue<std::string>* q, const dev_t device)
{
    auto result = std::make_shared<Node>();
    for(std::string dir;remove(q,dir);){
	auto child = disk_consumption(dir, device);
	result->size += child->size;
	result->children.push_back(child);
    }
    return result;
}

static NodePtr top_level(std::string dir, const int threads)
{
    if(dir.size() > 1 && dir.back() == '/'){
	dir = dir.substr(0,dir.size()-1);
    }
    struct stat buf;
    if(stat(dir.c_str(),&buf)){
	throw std::runtime_error("cannot stat directory: " + dir);
    }
    if((buf.st_mode & S_IFMT) != S_IFDIR){
	throw std::runtime_error(dir + " is not a directory");
    }

    std::queue<std::string> q;

    auto q_pusher = [&](std::string sub,dev_t) -> NodePtr {
			q.push(sub);
			return std::make_shared<Node>();
		    };
    
    const dev_t device = buf.st_dev;

    auto result = traverse_directory(dir, device, q_pusher);

    std::vector<std::future<NodePtr>> futures;

    for(int i=0; i<threads; i++){
	futures.emplace_back(std::async(std::launch::async,worker,&q,device));
    }

    for(auto& f : futures){
	auto job = f.get();
	for (auto child : job->children){
	    result->size += child->size;
	    result->children.push_back(child);
	}
    }

    return result;
}

static void display_results(NodePtr node,
			    const size_t reportable_size,
			    const bool elision)
{
    std::sort(node->children.begin(), node->children.end(),
	      [](const NodePtr&a, const NodePtr& b) -> bool {
		  return a->size > b->size;
	      });
    
    if(node->size > reportable_size){

	auto gigs = 1.0 * node->size / GB;
	::printf("%s %.1f\n", node->fullpath.c_str(), gigs);

	if(elision && node->children.size() == 1){
	    while(node->children.size() == 1){
		node = node->children.at(0);
	    }
	    display_results(node, reportable_size, elision);
	    
	} else {
	    for (auto child : node->children){
		display_results(child, reportable_size, elision);
	    }
	}
    }
}

int main(int argc, char** argv)
{
    int threads = 4;
    size_t reportable_size = 1 * GB;

    try {

	bool elided = true;

	while(argc > 2 && argv[1][0] == '-'){
	    std::string option(argv[1]);
	    if(option == "-threads"){
		threads = std::stoi(argv[2]);
	    } else if(option == "-size"){
		reportable_size = std::stoi(argv[2]) * GB;
	    } else if(option == "-no-elision"){
		elided = false;
		argc--;
		argv++;
		continue;
	    } else {
		throw std::runtime_error("unknown option: " + option);
	    }
	    argv += 2;
	    argc -= 2;
	}

	display_results(top_level(argv[1], threads),
			reportable_size,
			elided);
	return 0;

    } catch(std::exception& e){
	::fprintf(stderr, "%s\n", e.what());
	return 1;
    }
}

