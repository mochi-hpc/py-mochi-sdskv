/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <pybind11/pybind11.h>
#include <string>
#include <vector>
#include <cstring>
#include <iostream>
#include <margo.h>
#include <sdskv-common.h>
#include <sdskv-server.h>

namespace py11 = pybind11;

typedef py11::capsule pymargo_instance_id;
typedef py11::capsule pyhg_addr_t;
typedef py11::capsule pysdskv_provider_t;

#define MID2CAPSULE(__mid)    py11::capsule((void*)(__mid),  "margo_instance_id", nullptr)
#define ADDR2CAPSULE(__addr)  py11::capsule((void*)(__addr), "hg_addr_t", nullptr)
#define SDSKVPR2CAPSULE(__pr) py11::capsule((void*)(__pr),   "pysdskv_provider_t", nullptr)

static pysdskv_provider_t pysdskv_provider_register(pymargo_instance_id mid, uint8_t provider_id) {
    sdskv_provider_t provider;
    int ret = sdskv_provider_register(mid, provider_id, SDSKV_ABT_POOL_DEFAULT, &provider);
    if(ret != 0) return py11::none();
    else return SDSKVPR2CAPSULE(provider);
}

static sdskv_database_id_t pysdskv_provider_add_database(
        pysdskv_provider_t provider,
        const std::string& name,
        const std::string& path,
        sdskv_db_type_t type) {
    sdskv_database_id_t id;
    int ret = sdskv_provider_add_database(
                provider, name.c_str(), path.c_str(), type, NULL, &id);
    if(ret != 0) return SDSKV_DATABASE_ID_INVALID;
    else return id;
}

static py11::object pysdskv_provider_list_databases(pysdskv_provider_t provider) {
    py11::list result;
    uint64_t num_db;
    if(SDSKV_SUCCESS != sdskv_provider_count_databases(provider, &num_db)) {
        return py11::none();
    }
    std::vector<sdskv_database_id_t> dbs(num_db);
    if(SDSKV_SUCCESS != sdskv_provider_list_databases(provider, dbs.data())) {
        return py11::none();
    }
    for(auto id : dbs) {
        result.append(id);
    }
    return result;
}

PYBIND11_MODULE(_pysdskvserver, m)
{
    py11::enum_<sdskv_db_type_t>(m,"database_type")
        .value("map", KVDB_MAP)
        .value("bwtree", KVDB_BWTREE)
        .value("leveldb", KVDB_LEVELDB)
        .value("berkeleydb", KVDB_BERKELEYDB)
    ;
    m.def("register", &pysdskv_provider_register);
    m.def("add_database", &pysdskv_provider_add_database);
    m.def("remove_database", [](pysdskv_provider_t pr, sdskv_database_id_t db_id) {
            return sdskv_provider_remove_database(pr, db_id); });
    m.def("remove_all_databases", [](pysdskv_provider_t pr) {
            return sdskv_provider_remove_all_databases(pr); });
    m.def("databases", &pysdskv_provider_list_databases);
}
