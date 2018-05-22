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
#include <sdskv-client.h>

BOOST_PYTHON_OPAQUE_SPECIALIZED_TYPE_ID(margo_instance)
BOOST_PYTHON_OPAQUE_SPECIALIZED_TYPE_ID(sdskv_provider_handle)
BOOST_PYTHON_OPAQUE_SPECIALIZED_TYPE_ID(sdskv_client)
BOOST_PYTHON_OPAQUE_SPECIALIZED_TYPE_ID(hg_addr)

namespace bpl = boost::python;

static sdskv_client_t pysdskv_client_init(margo_instance_id mid) {
    sdskv_client_t result = SDSKV_CLIENT_NULL;
    sdskv_client_init(mid, &result);
    return result;
}

static sdskv_provider_handle_t pysdskv_provider_handle_create(
        sdskv_client_t client,
        hg_addr_t addr,
        uint8_t provider_id) {

    sdskv_provider_handle_t providerHandle = SDSKV_PROVIDER_HANDLE_NULL;
    sdskv_provider_handle_create(client, addr, provider_id, &providerHandle);
    return providerHandle;
}

static sdskv_database_id_t pysdskv_open(
        sdskv_provider_handle_t ph,
        const std::string& db_name)
{
    sdskv_database_id_t id;
    int ret;
    Py_BEGIN_ALLOW_THREADS 
    ret = sdskv_open(ph, db_name.c_str(), &id);
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS) return SDSKV_DATABASE_ID_INVALID;
    return id;
}

static bpl::object pysdskv_get(
        sdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::string& key) 
{
    hg_size_t vsize;
    int ret;
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_length(ph, id, key.c_str(), key.size(), &vsize);
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS) {
        return bpl::object();
    }
    std::string value(vsize, '\0');
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_get(ph, id, key.c_str(), key.size(), (void*)value.data(), &vsize);
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS) return bpl::object();
    return bpl::object(value);
}

static bpl::object pysdskv_put(
        sdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::string& key,
        const std::string& value) 
{
    int ret;
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_put(ph, id, key.data(), key.size(), value.data(), value.size());
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS) return bpl::object(false);
    else return bpl::object(true);
}

static bpl::object pysdskv_exists(
        sdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::string& key)
{
    hg_size_t len;
    int ret;
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_length(ph, id, key.data(), key.size(), &len);
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS) return bpl::object(false);
    else return bpl::object(true);
}

static void pysdskv_erase(
        sdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::string& key) {
    Py_BEGIN_ALLOW_THREADS
    sdskv_erase(ph, id, key.data(), key.size());
    Py_END_ALLOW_THREADS
}

BOOST_PYTHON_MODULE(_pysdskvclient)
{
#define ret_policy_opaque bpl::return_value_policy<bpl::return_opaque_pointer>()

    bpl::opaque<sdskv_client>();
    bpl::opaque<sdskv_provider_handle>();
    bpl::def("client_init", &pysdskv_client_init, ret_policy_opaque);
    bpl::def("client_finalize", &sdskv_client_finalize);
    bpl::def("provider_handle_create", &pysdskv_provider_handle_create, ret_policy_opaque);
    bpl::def("provider_handle_ref_incr", &sdskv_provider_handle_ref_incr);
    bpl::def("provider_handle_release", &sdskv_provider_handle_release);
    bpl::def("open", &pysdskv_open);
    bpl::def("get", &pysdskv_get);
    bpl::def("put", &pysdskv_put);
    bpl::def("exists", &pysdskv_exists);
    bpl::def("erase", &pysdskv_erase);
    bpl::def("shutdown_service", &sdskv_shutdown_service);

#undef ret_policy_opaque
}
