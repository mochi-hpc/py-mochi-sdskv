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

BOOST_PYTHON_MODULE(_pysdskvserver)
{
#define ret_policy_opaque bpl::return_value_policy<bpl::return_opaque_pointer>()

    bpl::opaque<sdskv_server_context_t>();
    bpl::def("register", &pysdskv_provider_register, ret_policy_opaque);

#undef ret_policy_opaque
}
