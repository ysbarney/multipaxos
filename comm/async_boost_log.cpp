#include "async_boost_log.h"
		
async_boost_log::async_boost_log()
{
}

async_boost_log::~async_boost_log()
{
}

// Convert file path to only the filename
std::string path_to_filename(std::string path) 
{
   std::string nullstr;
   std::size_t found = path.find_last_of("/\\");
   if(found > path.size())
   		return nullstr;
   else
   		return path.substr(found+1);
}


bool async_boost_log::initialize(const std::string& logconfile)
{	
	logging::add_common_attributes();
	
	logging::register_simple_formatter_factory<severity_level, char>("Severity");
	logging::register_simple_filter_factory<severity_level, char>("Severity");

	std::ifstream file(logconfile.c_str());
	if (!file.is_open()) {
        throw std::runtime_error("Could not open logconfig file");
		return false;
	}
	
	logging::init_from_stream(file);
	return true;
}

