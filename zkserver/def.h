#pragma once

namespace phxkv
{

const uint64_t NullVersion = std::numeric_limits<uint64_t>::min();

enum class PhxKVStatus
{
    SUCC = 0,
    FAIL = -1,
    KEY_NOTEXIST = 1,
    VERSION_CONFLICT = -11,
    VERSION_NOTEXIST = -12,
    MASTER_REDIRECT = 10,
    NO_MASTER = 101,
};


enum KVOperatorType
{
    KVOperatorType_READ = 1,
    KVOperatorType_SET = 2,
    KVOperatorType_DELETE = 3,
    KVOperatorType_CREATE = 4,
};

enum KVClientRet {
    /*KVCLIENT_OK = 0,
    KVCLIENT_SYS_FAIL = -1,
    KVCLIENT_KEY_NOTEXIST = 1,
    KVCLIENT_KEY_VERSION_CONFLICT = -11,*/
    ZOK = 0, /*!< Everything is OK */

	/** System and server-side errors.
	 * This is never thrown by the server, it shouldn't be used other than
	 * to indicate a range. Specifically error codes greater than this
	 * value, but lesser than {@link #ZAPIERROR}, are system errors. */
	ZSYSTEMERROR = -1,
	ZRUNTIMEINCONSISTENCY = -2, /*!< A runtime inconsistency was found */
	ZDATAINCONSISTENCY = -3, /*!< A data inconsistency was found */
	ZCONNECTIONLOSS = -4, /*!< Connection to the server has been lost */
	ZMARSHALLINGERROR = -5, /*!< Error while marshalling or unmarshalling data */
	ZUNIMPLEMENTED = -6, /*!< Operation is unimplemented */
	ZOPERATIONTIMEOUT = -7, /*!< Operation timeout */
	ZBADARGUMENTS = -8, /*!< Invalid arguments */
	ZINVALIDSTATE = -9, /*!< Invliad zhandle state */

	/** API errors.
	 * This is never thrown by the server, it shouldn't be used other than
	 * to indicate a range. Specifically error codes greater than this
	 * value are API errors (while values less than this indicate a 
	 * {@link #ZSYSTEMERROR}).
	 */
	ZAPIERROR = -100,
	ZNONODE = -101, /*!< Node does not exist */
	ZNOAUTH = -102, /*!< Not authenticated */
	ZBADVERSION = -103, /*!< Version conflict */
	ZNOCHILDRENFOREPHEMERALS = -108, /*!< Ephemeral nodes may not have children */
	ZNODEEXISTS = -110, /*!< The node already exists */
	ZNOTEMPTY = -111, /*!< The node has children */
	ZSESSIONEXPIRED = -112, /*!< The session has been expired by the server */
	ZINVALIDCALLBACK = -113, /*!< Invalid callback specified */
	ZINVALIDACL = -114, /*!< Invalid ACL specified */
	ZAUTHFAILED = -115, /*!< Client authentication failed */
	ZCLOSING = -116, /*!< ZooKeeper is closing */
	ZNOTHING = -117, /*!< (not error) no server responses to process */
	ZSESSIONMOVED = -118 /*!<session moved to another server, so operation is ignored */ 
};

}

