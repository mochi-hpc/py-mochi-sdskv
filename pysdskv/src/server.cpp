/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#define BOOST_NO_AUTO_PTR
#include <boost/python.hpp>
#include <boost/python/return_opaque_pointer.hpp>
#include <boost/python/handle.hpp>
#include <boost/python/enum.hpp>
#include <boost/python/def.hpp>
#include <boost/python/module.hpp>
#include <boost/python/return_value_policy.hpp>
#include <string>
#include <vector>
#include <cstring>
#include <iostream>
#include <margo.h>
#include <sdskv-common.h>
#include <sdskv-server.h>

BOOST_PYTHON_OPAQUE_SPECIALIZED_TYPE_ID(margo_instance)
BOOST_PYTHON_OPAQUE_SPECIALIZED_TYPE_ID(sdskv_server_context_t)

namespace bpl = boost::python;

static sdskv_provider_t pysdskv_provider_register(margo_instance_id mid, uint8_t provider_id) {
    sdskv_provider_t provider;
    int ret = sdskv_provider_register(mid, provider_id, SDSKV_ABT_POOL_DEFAULT, &provider);
    if(ret != 0) return NULL;
    else return provider;
}

static sdskv_database_id_t pysdskv_provider_add_database(
        sdskv_provider_t provider,
        const std::string& name,
        const std::string& path,
        sdskv_db_type_t type) {
    sdskv_database_id_t id;
    int ret = sdskv_provider_add_database(
                provider, name.c_str(), path.c_str(), type, NULL, &id);
    if(ret != 0) return SDSKV_DATABASE_ID_INVALID;
    else return id;
}

static bpl::object pysdskv_provider_list_databases(sdskv_provider_t provider) {
    bpl::list result;
    uint64_t num_db;
    if(SDSKV_SUCCESS != sdskv_provider_count_databases(provider, &num_db)) {
        return bpl::object();
    }
    std::vector<sdskv_database_id_t> dbs(num_db);
    if(SDSKV_SUCCESS != sdskv_provider_list_databases(provider, dbs.data())) {
        return bpl::object();
    }
    for(auto id : dbs) {
        result.append(id);
    }
    return result;
}

BOOST_PYTHON_MODULE(_pysdskvserver)
{
#define ret_policy_opaque bpl::return_value_policy<bpl::return_opaque_pointer>()

    bpl::opaque<sdskv_server_context_t>();
    bpl::enum_<sdskv_db_type_t>("database_type")
        .value("map", KVDB_MAP)
        .value("bwtree", KVDB_BWTREE)
        .value("leveldb", KVDB_LEVELDB)
        .value("berkeleydb", KVDB_BERKELEYDB)
    ;
    bpl::def("register", &pysdskv_provider_register, ret_policy_opaque);
    bpl::def("add_database", &pysdskv_provider_add_database);
    bpl::def("remove_database", &sdskv_provider_remove_database);
    bpl::def("remove_all_databases", &sdskv_provider_remove_all_databases);
    bpl::def("databases", &pysdskv_provider_list_databases);
#undef ret_policy_opaque
}
