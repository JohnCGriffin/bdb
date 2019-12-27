
/*********************************************

 bdb - big disk branches on Linux/Mac
 
 Report back list of large directories with
 size larger than 1G. 

 Note that this purposely does not cross file
 systems, so using 'bdb /' will report on the
 root file system, not everything under its
 directory structure.  Likewise, it purposely
 avoids symlinks.

 options: -t N  (number of threads, default 4)

**********************************************/

#include <iostream>
#include <vector>
#include <exception>
#include <iomanip>
#include <future>
#include <mutex>
#include <queue>
#include <cstring>

#include <sys/stat.h>
#include <dirent.h>


size_t traverse_directory(const std::string dir,
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

void print_directory_summary(std::string dir, size_t bytes)
{
    static std::mutex print_mutex;
    std::lock_guard<std::mutex> guard(print_mutex);
    auto gigs = 1.0 * bytes / (1024 * 1024 * 1024);
    std::cout << dir << " " << std::fixed << std::setprecision(1) << gigs << "\n";
}

size_t disk_consumption(std::string dir, const dev_t device)
{
    const size_t SMALLEST_PRINTABLE = 1024 * 1024 * 1024;

    const auto result = traverse_directory(dir, device, disk_consumption);

    if(SMALLEST_PRINTABLE < result){
	print_directory_summary(dir, result);
    }

    return result;
}

bool remove(std::queue<std::string>* q, std::string& receiver)
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

size_t worker(std::queue<std::string>* q, dev_t device)
{
    size_t result = 0;
    for(std::string dir;remove(q,dir);){
	result += disk_consumption(dir, device);
    }
    return result;
}

void top_level(std::string dir, const int threads)
{
    if(dir.size() > 1 && dir.back() == '/'){
	dir = dir.substr(0,dir.size()-1);
    }
    struct stat buf;
    if(stat(dir.c_str(),&buf)){
	throw new std::runtime_error("cannot stat directory: " + dir);
    }
    if((buf.st_mode & S_IFMT) != S_IFDIR){
	throw new std::runtime_error(dir + " is not a directory");
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

    print_directory_summary(dir, size);
}

int main(int argc, char** argv)
{
    try {

	auto threads = 4;
	if (argc > 2 && strcmp("-t",argv[1]) == 0){
	    threads = atoi(argv[2]);
	    argc -= 2;
	    argv += 2;
	}
	auto top_dir(argc >= 2 ? argv[1] : ".");
	top_level(top_dir, threads);
	return 0;

    } catch(std::exception& e){
	std::cerr << e.what() << "\n";
	return 1;
    }
}

