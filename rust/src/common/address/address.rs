/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

use std::{fmt, str::FromStr};

#[cfg(feature = "grpc")]
use http::{Uri, uri::PathAndQuery};

#[cfg(feature = "grpc")]
use crate::error::ConnectionError;
use crate::common::{Error, Result};

#[cfg(feature = "grpc")]
#[derive(Clone, Hash, PartialEq, Eq, Default)]
pub struct Address {
    uri: Uri,
}

#[cfg(not(feature = "grpc"))]
#[derive(Clone, Hash, PartialEq, Eq, Default)]
pub struct Address {
    raw: String,
}

#[cfg(feature = "grpc")]
impl Address {
    const DEFAULT_PATH: &'static str = "/";

    pub(crate) fn into_uri(self) -> Uri {
        self.uri
    }

    pub(crate) fn uri_scheme(&self) -> Option<&http::uri::Scheme> {
        self.uri.scheme()
    }

    pub(crate) fn with_scheme(&self, scheme: http::uri::Scheme) -> Self {
        let mut parts = self.uri.clone().into_parts();
        parts.scheme = Some(scheme);
        if parts.path_and_query.is_none() {
            parts.path_and_query = Some(PathAndQuery::from_static(Self::DEFAULT_PATH));
        }
        Self { uri: Uri::from_parts(parts).expect("Expected valid URI after scheme change") }
    }
}

#[cfg(feature = "grpc")]
impl FromStr for Address {
    type Err = Error;

    fn from_str(address: &str) -> Result<Self> {
        let uri = address.parse::<Uri>()?;
        if uri.port().is_none() {
            return Err(Error::Connection(ConnectionError::MissingPort { address: address.to_owned() }));
        }
        Ok(Self { uri })
    }
}

#[cfg(feature = "grpc")]
impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.uri.authority().unwrap())
    }
}

#[cfg(feature = "grpc")]
impl fmt::Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.uri)
    }
}

#[cfg(not(feature = "grpc"))]
impl FromStr for Address {
    type Err = Error;

    fn from_str(address: &str) -> Result<Self> {
        Ok(Self { raw: address.to_owned() })
    }
}

#[cfg(not(feature = "grpc"))]
impl fmt::Display for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.raw)
    }
}

#[cfg(not(feature = "grpc"))]
impl fmt::Debug for Address {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}", self.raw)
    }
}
