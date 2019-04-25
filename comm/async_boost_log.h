#ifndef ASYNC_BOOST_LOG_H_
#define ASYNC_BOOST_LOG_H_

#include <boost/date_time/posix_time/posix_time_types.hpp>
#include <boost/log/trivial.hpp>
#include <boost/log/expressions.hpp>
#include <boost/log/sources/severity_logger.hpp>
#include <boost/log/sources/record_ostream.hpp>
#include <boost/log/utility/setup/file.hpp>
#include <boost/log/utility/setup/common_attributes.hpp>
#include <boost/log/support/date_time.hpp>
#include <boost/log/sources/channel_feature.hpp>
#include <boost/log/sources/channel_logger.hpp>
#include <string>
#include <boost/smart_ptr/shared_ptr.hpp>
#include <boost/log/common.hpp>
#include <boost/log/attributes.hpp>
#include <boost/log/sinks.hpp>
#include <boost/log/sources/logger.hpp>
#include <boost/log/sinks/text_file_backend.hpp>
#include <boost/log/sources/severity_channel_logger.hpp>
#include <boost/log/utility/setup/settings.hpp>
#include <boost/log/utility/setup/from_stream.hpp>
#include <boost/log/utility/setup/console.hpp>
#include <boost/smart_ptr/make_shared_object.hpp>

#include <fstream>
#include <stdexcept>


namespace logging = boost::log;
namespace src = boost::log::sources;
namespace expr = boost::log::expressions;
namespace keywords = boost::log::keywords;
namespace attrs = boost::log::attributes;
namespace sinks = boost::log::sinks;

using namespace logging::trivial;
using namespace std;

enum ELogPriority
{ 
    eLogDebug = 0, 
    eLogInfo = 1, 
    eLogWarning = 2, 
    eLogError = 3, 
    eLogFatalError = 4,
};

class async_boost_log {
public:
	async_boost_log();
	virtual ~async_boost_log();

	bool initialize(const std::string& logconfile);

	//void start();
	
private:
	std::string m_logconfile;
};

std::string path_to_filename(std::string path);

// Define a global logger
BOOST_LOG_INLINE_GLOBAL_LOGGER_CTOR_ARGS(paxos_logger, src::severity_channel_logger_mt<severity_level>, (keywords::channel = "Paxos"))
BOOST_LOG_INLINE_GLOBAL_LOGGER_CTOR_ARGS(zk_logger, src::severity_channel_logger_mt<severity_level>, (keywords::channel = "ZkServer"))


#define PAXOSLOG_TRACE BOOST_LOG_SEV(paxos_logger::get(), trace) << "[" << path_to_filename(__FILE__)<<":"<<__LINE__<<"]"
#define PAXOSLOG_DEBUG BOOST_LOG_SEV(paxos_logger::get(), debug) << "[" << path_to_filename(__FILE__)<<":"<<__LINE__<<"]"
#define PAXOSLOG_INFO BOOST_LOG_SEV(paxos_logger::get(), info) << "[" << path_to_filename(__FILE__)<<":"<<__LINE__<<"]"
#define PAXOSLOG_WARN BOOST_LOG_SEV(paxos_logger::get(), warning) << "[" << path_to_filename(__FILE__)<<":"<<__LINE__<<"]"
#define PAXOSLOG_ERROR BOOST_LOG_SEV(paxos_logger::get(), error) << "[" <<path_to_filename(__FILE__)<<":"<<__LINE__<<"]"
#define PAXOSLOG_FATAL BOOST_LOG_SEV(paxos_logger::get(), fatal) << "[" << path_to_filename(__FILE__)<<":"<<__LINE__<<"]"

#define ZKLOG_TRACE BOOST_LOG_SEV(zk_logger::get(), trace) << "[" << path_to_filename(__FILE__)<<":"<<__LINE__<<"]"
#define ZKLOG_DEBUG BOOST_LOG_SEV(zk_logger::get(), debug) << "[" << path_to_filename(__FILE__)<<":"<<__LINE__<<"]"
#define ZKLOG_INFO BOOST_LOG_SEV(zk_logger::get(), info) << "[" << path_to_filename(__FILE__)<<":"<<__LINE__<<"]"
#define ZKLOG_WARN BOOST_LOG_SEV(zk_logger::get(), warning) << "[" << path_to_filename(__FILE__)<<":"<<__LINE__<<"]"
#define ZKLOG_ERROR BOOST_LOG_SEV(zk_logger::get(), error) << "[" << path_to_filename(__FILE__)<<":"<<__LINE__<<"]"
#define ZKLOG_FATAL BOOST_LOG_SEV(zk_logger::get(), fatal) << "[" << path_to_filename(__FILE__)<<":"<<__LINE__<<"]"

#endif
