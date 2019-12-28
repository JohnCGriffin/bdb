
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

#include <iostream>
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

static int threads = 4;
static size_t smallest_reportable_size = 1 * GB;

struct DirSize {
    std::string fullpath;
    size_t size;
};

static std::vector<DirSize> summary;

static void add_dir_size(std::string full_path, const size_t size){
    static std::mutex m;
    std::lock_guard<std::mutex> guard(m);
    summary.push_back({ full_path, size });
}

static size_t
traverse_directory(const std::string dir,
		   const dev_t device,
		   const std::function<size_t(std::string,dev_t)> f)
{
    size_t result = 0;

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

		result += f(path,device);

	    } else if(entry->d_type == DT_REG){
	
		result += buf.st_blocks * 512; // man 2 stat
	    }

	}
	
	closedir(dirp);
    }

    return result;
}

static size_t disk_consumption(std::string dir, const dev_t device)
{
    const auto result = traverse_directory(dir, device, disk_consumption);

    if(smallest_reportable_size < result){
	add_dir_size(dir,result);
    }

    return result;
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

static size_t worker(std::queue<std::string>* q, const dev_t device)
{
    size_t result = 0;
    for(std::string dir;remove(q,dir);){
	result += disk_consumption(dir, device);
    }
    return result;
}

static void top_level(std::string dir)
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

    auto q_pusher = [&](std::string sub,dev_t) { q.push(sub); return 0; };
    
    const dev_t device = buf.st_dev;

    auto size = traverse_directory(dir, device, q_pusher);

    std::vector<std::future<size_t>> futures;

    for(int i=0; i<threads; i++){
	futures.emplace_back(std::async(std::launch::async,worker,&q,device));
    }

    for(auto& f : futures){
	size += f.get();
    }

    add_dir_size(dir, size);
}

static void display_results()
{
    std::sort(summary.begin(), summary.end(),
	      [](const DirSize&a, const DirSize& b) -> bool {
		  return a.size > b.size;
	      });

    for(auto ds : summary){
	auto gigs = 1.0 * ds.size / GB;
	std::cout << ds.fullpath << " " << std::fixed << std::setprecision(1) << gigs << "\n";
    }
}

int main(int argc, char** argv)
{
    try {

	while(argc > 2 && argv[1][0] == '-'){
	    std::string option(argv[1]);
	    if(option == "-threads"){
		threads = std::stoi(argv[2]);
	    } else if(option == "-size"){
		smallest_reportable_size = std::stoi(argv[2]) * GB;
	    } else {
		throw std::runtime_error("unknown option: " + option);
	    }
	    argv += 2;
	    argc -= 2;
	}

	auto top_dir(argc >= 2 ? argv[1] : ".");
	top_level(top_dir);
	display_results();
	return 0;

    } catch(std::exception& e){
	std::cerr << e.what() << "\n";
	return 1;
    }
}

