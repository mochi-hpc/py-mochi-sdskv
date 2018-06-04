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
#include <sdskv-client.h>

namespace py11 = pybind11;

typedef py11::capsule pymargo_instance_id;
typedef py11::capsule pyhg_addr_t;
typedef py11::capsule pysdskv_provider_handle_t;
typedef py11::capsule pysdskv_client_t;

#define MID2CAPSULE(__mid)    py11::capsule((void*)(__mid),  "margo_instance_id", nullptr)
#define ADDR2CAPSULE(__addr)  py11::capsule((void*)(__addr), "hg_addr_t", nullptr)
#define SDSKVPH2CAPSULE(__ph) py11::capsule((void*)(__ph),   "sdskv_provider_handle_t", nullptr)
#define SDSKVCL2CAPSULE(__cl) py11::capsule((void*)(__cl),   "sdskv_client_t", nullptr)

static pysdskv_client_t pysdskv_client_init(pymargo_instance_id mid) {
    sdskv_client_t result = SDSKV_CLIENT_NULL;
    sdskv_client_init(mid, &result);
    return SDSKVCL2CAPSULE(result);
}

static pysdskv_provider_handle_t pysdskv_provider_handle_create(
        pysdskv_client_t client,
        pyhg_addr_t addr,
        uint8_t provider_id) {

    sdskv_provider_handle_t providerHandle = SDSKV_PROVIDER_HANDLE_NULL;
    sdskv_provider_handle_create(client, addr, provider_id, &providerHandle);
    return SDSKVPH2CAPSULE(providerHandle);
}

static sdskv_database_id_t pysdskv_open(
        pysdskv_provider_handle_t ph,
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

static py11::object pysdskv_get(
        pysdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::string& key) 
{
    hg_size_t vsize;
    int ret;
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_length(ph, id, key.c_str(), key.size(), &vsize);
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS) {
        return py11::none();
    }
    std::string value(vsize, '\0');
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_get(ph, id, key.c_str(), key.size(), (void*)value.data(), &vsize);
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS) return py11::none();
    return py11::cast(value);
}

static py11::object pysdskv_put(
        pysdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::string& key,
        const std::string& value) 
{
    int ret;
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_put(ph, id, key.data(), key.size(), value.data(), value.size());
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS) return py11::cast(false);
    else return py11::cast(true);
}

static py11::object pysdskv_exists(
        pysdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::string& key)
{
    hg_size_t len;
    int ret;
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_length(ph, id, key.data(), key.size(), &len);
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS) return py11::cast(false);
    else return py11::cast(true);
}

static void pysdskv_erase(
        pysdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::string& key) {
    Py_BEGIN_ALLOW_THREADS
    sdskv_erase(ph, id, key.data(), key.size());
    Py_END_ALLOW_THREADS
}

PYBIND11_MODULE(_pysdskvclient, m)
{
    m.def("client_init", &pysdskv_client_init);
    m.def("client_finalize", [](pysdskv_client_t clt) {
            return sdskv_client_finalize(clt); });
    m.def("provider_handle_create", &pysdskv_provider_handle_create);
    m.def("provider_handle_ref_incr", [](pysdskv_provider_handle_t ph) {
            return sdskv_provider_handle_ref_incr(ph); });
    m.def("provider_handle_release", [](pysdskv_provider_handle_t ph) {
            return sdskv_provider_handle_release(ph); });
    m.def("open", &pysdskv_open);
    m.def("get", &pysdskv_get);
    m.def("put", &pysdskv_put);
    m.def("exists", &pysdskv_exists);
    m.def("erase", &pysdskv_erase);
    m.def("shutdown_service", [](pysdskv_client_t clt, pyhg_addr_t addr) {
            return sdskv_shutdown_service(clt, addr); });
}
