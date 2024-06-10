/*
 *  Copyright (c) 2023-2024, NVIDIA CORPORATION.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  Licensed under the Apache License v2.0 with LLVM Exceptions.
 *  See LICENSE.txt for license information.
 *
 *  SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
 */

use std::ffi::c_void;
use std::ffi::CString;
use std::mem::size_of;
use std::path::PathBuf;
use std::ptr::{null, null_mut};

use crate::nvtxw_bindings;

pub use crate::nvtxw_bindings::nvtxPayloadData_t as PayloadData;
pub use crate::nvtxw_bindings::nvtxPayloadSchemaAttr_t as PayloadSchemaAttr;
pub use crate::nvtxw_bindings::nvtxPayloadSchemaEntry_t as PayloadSchemaEntry;
pub use crate::nvtxw_bindings::nvtxwInterfaceCore_t as InterfaceHandle;
pub use crate::nvtxw_bindings::nvtxwResultCode_t as ResultCode;
use crate::nvtxw_bindings::nvtxwSessionAttributes_t as SessionAttributes;
pub use crate::nvtxw_bindings::nvtxwSessionHandle_t as SessionHandle;
use crate::nvtxw_bindings::nvtxwStreamAttributes_t as StreamAttributes;
pub use crate::nvtxw_bindings::nvtxwStreamHandle_t as StreamHandle;

use crate::nvtxw_bindings::NVTXW3_INIT_MODE_LIBRARY_FILENAME;
use crate::nvtxw_bindings::NVTXW3_INTERFACE_ID_CORE_V1;
use crate::nvtxw_bindings::NVTXW3_RESULT_FAILED;
use crate::nvtxw_bindings::NVTXW3_RESULT_INVALID_ARGUMENT;
use crate::nvtxw_bindings::NVTXW3_RESULT_SUCCESS;
use crate::nvtxw_bindings::NVTXW3_STREAM_ORDERING_SKID_NONE;

pub const NVTX_PAYLOAD_ENTRY_FLAG_POINTER: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_ENTRY_FLAG_POINTER as u64;
pub const NVTX_PAYLOAD_ENTRY_FLAG_EVENT_MESSAGE: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_ENTRY_FLAG_EVENT_MESSAGE as u64;
pub const NVTX_PAYLOAD_ENTRY_FLAG_ARRAY_ZERO_TERMINATED: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_ENTRY_FLAG_ARRAY_ZERO_TERMINATED as u64;
pub const NVTX_PAYLOAD_ENTRY_FLAG_EVENT_TIMESTAMP: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_ENTRY_FLAG_EVENT_TIMESTAMP as u64;
pub const NVTX_PAYLOAD_ENTRY_FLAG_RANGE_BEGIN: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_ENTRY_FLAG_RANGE_BEGIN as u64;
pub const NVTX_PAYLOAD_ENTRY_FLAG_RANGE_END: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_ENTRY_FLAG_RANGE_END as u64;
pub const NVTX_PAYLOAD_ENTRY_TYPE_UINT64: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_ENTRY_TYPE_UINT64 as u64;
pub const NVTX_PAYLOAD_ENTRY_TYPE_NVTX_COLOR: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_ENTRY_TYPE_NVTX_COLOR as u64;
pub const NVTX_PAYLOAD_ENTRY_TYPE_CSTRING: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_ENTRY_TYPE_CSTRING as u64;
pub const NVTX_PAYLOAD_SCHEMA_TYPE_STATIC: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_SCHEMA_TYPE_STATIC as u64;
pub const NVTX_PAYLOAD_SCHEMA_TYPE_DYNAMIC: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_SCHEMA_TYPE_DYNAMIC as u64;
pub const NVTX_PAYLOAD_SCHEMA_FLAG_NONE: u64 = nvtxw_bindings::NVTX_PAYLOAD_SCHEMA_FLAG_NONE as u64;
pub const NVTX_PAYLOAD_SCHEMA_FLAG_REFERENCED: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_SCHEMA_FLAG_REFERENCED as u64;
pub const NVTX_PAYLOAD_SCHEMA_ATTR_NAME: u64 = nvtxw_bindings::NVTX_PAYLOAD_SCHEMA_ATTR_NAME as u64;
pub const NVTX_PAYLOAD_SCHEMA_ATTR_TYPE: u64 = nvtxw_bindings::NVTX_PAYLOAD_SCHEMA_ATTR_TYPE as u64;
pub const NVTX_PAYLOAD_SCHEMA_ATTR_FLAGS: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_SCHEMA_ATTR_FLAGS as u64;
pub const NVTX_PAYLOAD_SCHEMA_ATTR_ENTRIES: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_SCHEMA_ATTR_ENTRIES as u64;
pub const NVTX_PAYLOAD_SCHEMA_ATTR_NUM_ENTRIES: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_SCHEMA_ATTR_NUM_ENTRIES as u64;
pub const NVTX_PAYLOAD_SCHEMA_ATTR_STATIC_SIZE: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_SCHEMA_ATTR_STATIC_SIZE as u64;
pub const NVTX_PAYLOAD_SCHEMA_ATTR_SCHEMA_ID: u64 =
    nvtxw_bindings::NVTX_PAYLOAD_SCHEMA_ATTR_SCHEMA_ID as u64;

pub const NVTXW3_STREAM_ORDER_INTERLEAVING_NONE: i16 = 0;
pub const NVTXW3_STREAM_ORDERING_TYPE_UNKNOWN: i16 = 0;

fn check(result: ResultCode) -> Result<(), ResultCode> {
    if result != NVTXW3_RESULT_SUCCESS {
        return Err(result);
    }
    Ok(())
}

pub fn initialize_simple() -> Result<InterfaceHandle, ResultCode> {
    let so_name = "libNvtxwBackend.so";
    let c_mode_string = CString::new(so_name).expect("modeString CString::new failed");
    let cptr_mode_string = c_mode_string.as_ptr();

    let mut get_interface_func: nvtxw_bindings::nvtxwGetInterface_t = Default::default();
    check(unsafe {
        nvtxw_bindings::nvtxwInitialize(
            NVTXW3_INIT_MODE_LIBRARY_FILENAME,
            cptr_mode_string,
            &mut get_interface_func,
            null_mut(),
        )
    })?;

    let get_interface = get_interface_func.ok_or(NVTXW3_RESULT_FAILED)?;

    let mut interface_void: *const c_void = null();
    check(unsafe { get_interface(NVTXW3_INTERFACE_ID_CORE_V1, &mut interface_void) })?;

    if interface_void.is_null() {
        return Err(NVTXW3_RESULT_FAILED);
    }

    let ptr_interface = interface_void as *const nvtxw_bindings::nvtxwInterfaceCore_t;
    let interface;
    unsafe {
        interface = *ptr_interface;
    }

    Ok(interface)
}

pub fn session_begin_simple(
    interface: InterfaceHandle,
    o_output: Option<PathBuf>,
    o_merge: Option<PathBuf>,
) -> Result<SessionHandle, ResultCode> {
    let func = interface
        .SessionBegin
        .ok_or(NVTXW3_RESULT_INVALID_ARGUMENT)?;

    let mut c_output: CString = CString::new("").expect("CString::new failed");
    if let Some(os_output) = o_output {
        let s_output = os_output.to_string_lossy();
        let session_name = match s_output.strip_suffix(".nsys-rep") {
            Some(s) => s,
            None => &s_output,
        };
        c_output = CString::new(session_name).expect("CString::new failed");
    }

    let mut c_config: CString = CString::new("").expect("CString::new failed");
    if let Some(os_merge) = o_merge {
        let s_merge = os_merge.to_string_lossy();
        let s_config = format!("ReportMerge={}", s_merge);

        c_config = CString::new(s_config).expect("CString::new failed");
    }

    let session_attr = SessionAttributes {
        struct_size: size_of::<SessionAttributes>(),
        name: if c_output.is_empty() {
            null_mut()
        } else {
            c_output.as_ptr()
        },
        configString: if c_config.is_empty() {
            null_mut()
        } else {
            c_config.as_ptr()
        },
    };

    let mut session: SessionHandle = Default::default();
    check(unsafe { func(&mut session, &session_attr) })?;

    if session.opaque.is_null() {
        return Err(NVTXW3_RESULT_FAILED);
    }

    Ok(session)
}

pub fn stream_open_simple(
    interface: InterfaceHandle,
    session: SessionHandle,
    stream_name: String,
    domain_name: String,
) -> Result<StreamHandle, ResultCode> {
    let func = interface.StreamOpen.ok_or(NVTXW3_RESULT_INVALID_ARGUMENT)?;

    let c_stream_name = CString::new(stream_name).expect("CString::new failed");
    let c_domain_name = CString::new(domain_name).expect("CString::new failed");

    let stream_attr = StreamAttributes {
        struct_size: size_of::<StreamAttributes>(),
        name: c_stream_name.as_ptr(),
        nvtxDomainName: c_domain_name.as_ptr(),
        eventScopePath: null(),
        orderInterleaving: NVTXW3_STREAM_ORDER_INTERLEAVING_NONE,
        orderingType: NVTXW3_STREAM_ORDERING_TYPE_UNKNOWN,
        orderingSkid: NVTXW3_STREAM_ORDERING_SKID_NONE,
        orderingSkidAmount: 0,
    };

    let mut stream: StreamHandle = Default::default();
    check(unsafe { func(&mut stream, session, &stream_attr) })?;

    if stream.opaque.is_null() {
        return Err(NVTXW3_RESULT_FAILED);
    }

    Ok(stream)
}

pub fn schema_register(
    interface: InterfaceHandle,
    stream: StreamHandle,
    schema_attr: &PayloadSchemaAttr,
) -> Result<(), ResultCode> {
    let func = interface
        .SchemaRegister
        .ok_or(NVTXW3_RESULT_INVALID_ARGUMENT)?;

    check(unsafe { func(stream, schema_attr) })
}

pub fn event_write(
    interface: InterfaceHandle,
    stream: StreamHandle,
    events: &[PayloadData],
) -> Result<(), ResultCode> {
    let func = interface.EventWrite.ok_or(NVTXW3_RESULT_INVALID_ARGUMENT)?;

    let count = events.len();
    let ptr = events.as_ptr();

    check(unsafe { func(stream, ptr, count) })
}

pub fn stream_end(interface: InterfaceHandle, stream: StreamHandle) -> Result<(), ResultCode> {
    let func = interface
        .StreamClose
        .ok_or(NVTXW3_RESULT_INVALID_ARGUMENT)?;

    check(unsafe { func(stream) })
}

pub fn session_end(interface: InterfaceHandle, session: SessionHandle) -> Result<(), ResultCode> {
    let func = interface.SessionEnd.ok_or(NVTXW3_RESULT_INVALID_ARGUMENT)?;

    check(unsafe { func(session) })
}
