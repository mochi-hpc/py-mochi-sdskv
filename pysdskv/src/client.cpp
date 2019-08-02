/*
 * (C) 2018 The University of Chicago
 * 
 * See COPYRIGHT in top-level directory.
 */
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <sstream>
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

static std::vector<std::pair<std::string,sdskv_database_id_t>> pysdskv_list_databases(
        pysdskv_provider_handle_t ph)
{
    int ret;
    size_t count;
    std::vector<char*> db_names;
    std::vector<sdskv_database_id_t> db_ids;
    std::vector<std::pair<std::string,sdskv_database_id_t>> result;

    ret = sdskv_count_databases(ph, &count);
    if(ret !=  SDSKV_SUCCESS)
        throw std::runtime_error(std::string("sdskv_count_databases returned error ") 
                + std::to_string(ret));
    db_names.resize(count);
    db_ids.resize(count);
    ret = sdskv_list_databases(ph, &count, db_names.data(), db_ids.data());
    if(ret !=  SDSKV_SUCCESS)
        throw std::runtime_error(std::string("sdskv_list_databases returned error ") 
                + std::to_string(ret));
    result.resize(count);

    for(unsigned i=0; i < count; i++) {
        result[i].first = db_names[i];
        result[i].second = db_ids[i];
    }
    for(unsigned i=0; i < db_names.size(); i++) free(db_names[i]);

    return result;
}

static py11::object pysdskv_get(
        pysdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::string& key,
        hg_size_t vsize) 
{
    int ret;
    if(vsize == 0) {
        Py_BEGIN_ALLOW_THREADS
        ret = sdskv_length(ph, id, key.c_str(), key.size(), &vsize);
        Py_END_ALLOW_THREADS
        if(ret == SDSKV_ERR_UNKNOWN_KEY) return py11::none();
        if(ret != SDSKV_SUCCESS) {
            throw std::runtime_error(std::string("sdskv_length returned ")+std::to_string(ret));
        }
    }
    std::string value(vsize, '\0');
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_get(ph, id, key.c_str(), key.size(), (void*)value.data(), &vsize);
    Py_END_ALLOW_THREADS
    if(ret == SDSKV_ERR_UNKNOWN_KEY) return py11::none();
    if(ret != SDSKV_SUCCESS)
        throw std::runtime_error(std::string("sdskv_get returned ")+std::to_string(ret));
    return py11::cast(value);
}

static py11::object pysdskv_get_multi(
        pysdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::vector<std::string>& keys,
        std::vector<hg_size_t>& val_sizes) 
{
    std::vector<hg_size_t> vsizes(keys.size());
    std::vector<const void*> keys_ptr(keys.size());
    std::vector<hg_size_t> keys_size(keys.size());
    for(unsigned i=0; i < keys.size(); i++) {
        keys_ptr[i]  = keys[i].data();
        keys_size[i] = keys[i].size();
    }
    int ret;
    if(val_sizes[0] == 0) {
        Py_BEGIN_ALLOW_THREADS
        ret = sdskv_length_multi(ph, id, keys.size(),
                keys_ptr.data(), keys_size.data(), val_sizes.data());
        Py_END_ALLOW_THREADS
        if(ret != SDSKV_SUCCESS) {
            throw std::runtime_error(std::string("sdskv_length_multi returned ")
                    + std::to_string(ret));
        }
    }
    std::vector<std::string> values(keys.size());
    std::vector<void*> val_ptrs(keys.size());
    for(unsigned i=0; i < keys.size(); i++) {
        values[i].resize(val_sizes[i]);
        val_ptrs[i] = const_cast<char*>(values[i].data());
    }
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_get_multi(ph, id, keys.size(),
            keys_ptr.data(), keys_size.data(),
            val_ptrs.data(), val_sizes.data());
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS) {
        throw std::runtime_error(std::string("sdskv_get_multi returned ")
                + std::to_string(ret));
    }
    for(unsigned i=0; i < keys.size(); i++) {
        values[i].resize(val_sizes[i]);
    }
    return py11::cast(values);
}

static py11::object pysdskv_length(
        pysdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::string& key) 
{
    hg_size_t vsize;
    int ret;
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_length(ph, id, key.c_str(), key.size(), &vsize);
    Py_END_ALLOW_THREADS
    if(ret == SDSKV_ERR_UNKNOWN_KEY) return py11::none();
    if(ret != SDSKV_SUCCESS) {
        throw std::runtime_error(std::string("sdskv_length returned ")
                + std::to_string(ret));
    }
    return py11::cast(vsize);
}

static py11::object pysdskv_length_multi(
        pysdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::vector<std::string>& keys)
{
    std::vector<hg_size_t> vsizes(keys.size());
    std::vector<hg_size_t> keys_size(keys.size());
    std::vector<const void*> keys_ptrs(keys.size());
    for(unsigned i=0; i<keys.size(); i++) {
        keys_ptrs[i] = keys[i].data();
        keys_size[i] = keys[i].size();
    }
    int ret;
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_length_multi(ph, id, keys.size(),
            keys_ptrs.data(), keys_size.data(), vsizes.data());
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS) {
        throw std::runtime_error(std::string("sdskv_length_multi returned ")
                + std::to_string(ret));
    }
    return py11::cast(vsizes);
}

static void pysdskv_put(
        pysdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::string& key,
        const std::string& value) 
{
    int ret;
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_put(ph, id, key.data(), key.size(), value.data(), value.size());
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS) 
        throw std::runtime_error(std::string("sdskv_put returned ")
                + std::to_string(ret));
}

static void pysdskv_put_multi(
        pysdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::vector<std::string>& keys,
        const std::vector<std::string>& values) 
{
    int ret;
    std::vector<const void*> keys_ptrs(keys.size());
    std::vector<hg_size_t>   keys_size(keys.size());
    std::vector<const void*> vals_ptrs(values.size());
    std::vector<hg_size_t>   vals_size(values.size());
    for(unsigned i=0; i < keys.size(); i++) {
        keys_ptrs[i] = keys[i].data();
        keys_size[i] = keys[i].size();
        vals_ptrs[i] = values[i].data();
        vals_size[i] = values[i].size();
    }
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_put_multi(ph, id, keys.size(),
            keys_ptrs.data(), keys_size.data(),
            vals_ptrs.data(), vals_size.data());
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS)
        throw std::runtime_error(std::string("sdskv_put_multi returned ")
                + std::to_string(ret));
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
    if(ret == SDSKV_ERR_UNKNOWN_KEY) return py11::cast(false);
    if(ret == SDSKV_SUCCESS) return py11::cast(true);
    throw std::runtime_error(std::string("sdskv_length returned ")
            + std::to_string(ret));
}

static void pysdskv_erase(
        pysdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::string& key) {
    int ret;
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_erase(ph, id, key.data(), key.size());
    Py_END_ALLOW_THREADS
    if(ret == SDSKV_SUCCESS) return;
    throw std::runtime_error(std::string("sdskv_erase returned ")+std::to_string(ret));
}

static std::vector<std::string> pysdskv_list_keys(
        pysdskv_provider_handle_t ph,
        sdskv_database_id_t id,
        const std::string& start_key,
        const std::string& prefix,
        hg_size_t max_keys,
        hg_size_t key_size) {

    int ret;
    if(max_keys == 0)
        return std::vector<std::string>();

    std::vector<hg_size_t> key_sizes(max_keys, key_size);
    std::vector<std::string> keys(max_keys);
    std::vector<void*> keys_addr(max_keys, nullptr);

    if(key_size == 0) {
        ret = sdskv_list_keys_with_prefix(ph, id,
                start_key.data(), start_key.size(),
                prefix.data(), prefix.size(),
                nullptr,
                key_sizes.data(),
                &max_keys);
        if(ret != SDSKV_SUCCESS && ret != SDSKV_ERR_SIZE) {
            throw std::runtime_error(std::string("sdskv_list_keys_with_prefix returned ")+std::to_string(ret));
        }
    }

    for(unsigned i = 0; i < max_keys; i++) {
        keys_addr[i] = const_cast<char*>(keys[i].data());
        keys[i].resize(key_sizes[i]);
    }

    ret = sdskv_list_keys_with_prefix(ph, id,
            start_key.data(), start_key.size(),
            prefix.data(), prefix.size(),
            keys_addr.data(),
            key_sizes.data(),
            &max_keys);
    keys.resize(max_keys);
    for(unsigned i = 0; i < max_keys; i++) {
        keys[i].resize(key_sizes[i]);
    }

    if(ret != SDSKV_SUCCESS)
        throw std::runtime_error(std::string("sdskv_list_keys_with_prefix returned ")+std::to_string(ret));

    return keys;
}

static std::pair<
        std::vector<std::string>,
        std::vector<std::string>
        > pysdskv_list_keyvals(
            pysdskv_provider_handle_t ph,
            sdskv_database_id_t id,
            const std::string& start_key,
            const std::string& prefix,
            hg_size_t max_keys,
            hg_size_t key_size,
            hg_size_t val_size) {

    int ret;
    std::vector<hg_size_t> key_sizes(max_keys, key_size);
    std::vector<hg_size_t> val_sizes(max_keys, val_size);

    std::pair<std::vector<std::string>,std::vector<std::string>> result;
    if(max_keys == 0)
        return result;

    result.first.resize(max_keys);
    result.second.resize(max_keys);

    std::vector<std::string>& keys = result.first;
    std::vector<void*> keys_addr(max_keys, nullptr);
    std::vector<std::string>& vals = result.second;
    std::vector<void*> vals_addr(max_keys, nullptr);

    if(key_size == 0 || val_size == 0) {
        ret = sdskv_list_keyvals_with_prefix(ph, id,
                start_key.data(), start_key.size(),
                prefix.data(), prefix.size(),
                nullptr,
                key_sizes.data(),
                nullptr,
                val_sizes.data(),
                &max_keys);
        if(ret != SDSKV_SUCCESS && ret != SDSKV_ERR_SIZE) {
            throw std::runtime_error(std::string("sdskv_list_keyvals_with_prefix returned ")+std::to_string(ret));
        }
    }

    for(unsigned i = 0; i < max_keys; i++) {
        keys[i].resize(key_sizes[i]);
        keys_addr[i] = const_cast<char*>(keys[i].data());
        vals[i].resize(val_sizes[i]);
        vals_addr[i] = const_cast<char*>(vals[i].data());
    }

    ret = sdskv_list_keyvals_with_prefix(ph, id,
            start_key.data(), start_key.size(),
            prefix.data(), prefix.size(),
            keys_addr.data(),
            key_sizes.data(),
            vals_addr.data(),
            val_sizes.data(),
            &max_keys);

    keys.resize(max_keys);
    vals.resize(max_keys);
    for(unsigned i = 0; i < max_keys; i++) {
        keys[i].resize(key_sizes[i]);
        vals[i].resize(val_sizes[i]);
    }

    if(ret != SDSKV_SUCCESS)
        throw std::runtime_error(std::string("sdskv_list_keys_with_prefix returned ")+std::to_string(ret));

    return result;
}

static void pysdskv_migrate_database(
        pysdskv_provider_handle_t ph,
        sdskv_database_id_t source_id,
        const std::string& dest_addr,
        uint16_t dest_provider_id,
        const std::string& dest_root,
        bool remove_origin) {

    int ret;
    Py_BEGIN_ALLOW_THREADS
    ret = sdskv_migrate_database(
            ph, source_id, dest_addr.c_str(),
            dest_provider_id, dest_root.c_str(),
            remove_origin ? SDSKV_REMOVE_ORIGINAL : SDSKV_KEEP_ORIGINAL);
    Py_END_ALLOW_THREADS
    if(ret != SDSKV_SUCCESS)
        throw std::runtime_error(std::string("sdskv_migrate_database returned ")+std::to_string(ret));
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
    m.def("list_databases", &pysdskv_list_databases);
    m.def("get", &pysdskv_get);
    m.def("get_multi", &pysdskv_get_multi);
    m.def("length", &pysdskv_length);
    m.def("length_multi", &pysdskv_length_multi);
    m.def("put", &pysdskv_put);
    m.def("put_multi", &pysdskv_put_multi);
    m.def("exists", &pysdskv_exists);
    m.def("erase", &pysdskv_erase);
    m.def("list_keys", &pysdskv_list_keys);
    m.def("list_keyvals", &pysdskv_list_keyvals);
    m.def("migrate_database", &pysdskv_migrate_database);
    m.def("shutdown_service", [](pysdskv_client_t clt, pyhg_addr_t addr) {
            return sdskv_shutdown_service(clt, addr); });
}
