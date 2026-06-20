// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Builds a Paimon catalog [`Options`] from the lake catalog properties carried
//! on a Fluss table.
//!
//! Fluss stores the lake catalog config on each lake-enabled table's properties
//! under the `table.datalake.<format>.` prefix; the Fluss client strips that
//! prefix (see `TableConfig::get_lake_catalog_properties`) leaving keys such as
//! `warehouse`, `metastore`, `uri`, and free-form `s3.*` / `oss.*` storage
//! credentials. Those keys map one-to-one onto Paimon's [`Options`], so building
//! the catalog config is a direct copy plus a presence check on `warehouse`.

use std::collections::HashMap;

use paimon::{CatalogOptions, Options};

use crate::error::{FlussLakeError, Result};

/// Paimon catalog configuration derived from a Fluss table's lake properties.
#[derive(Debug, Clone)]
pub struct LakeCatalogConfig {
    options: Options,
}

impl LakeCatalogConfig {
    /// Builds the config from already-prefix-stripped lake catalog properties
    /// (the map returned by `TableConfig::get_lake_catalog_properties`).
    ///
    /// Every entry is passed through verbatim to Paimon's [`Options`]; storage
    /// credentials (`s3.*` / `oss.*`) flow straight to OpenDAL there. The only
    /// validation is that `warehouse` is present, since the catalog cannot be
    /// opened without it.
    pub fn from_catalog_properties(props: &HashMap<String, String>) -> Result<Self> {
        // Integration tests that drive a real tiering job behind an S3-compatible
        // store rewrite the container-internal S3 endpoint to the host-mapped one
        // here, the single point every catalog config flows through. No-op (and
        // not compiled) outside the `integration_tests` feature.
        #[cfg(feature = "integration_tests")]
        let props = &{
            let mut props = props.clone();
            crate::test_overrides::apply_s3_endpoint_override(&mut props);
            props
        };

        let mut options = Options::new();
        for (key, value) in props {
            options.set(key.clone(), value.clone());
        }
        if options
            .get(CatalogOptions::WAREHOUSE)
            .map(|w| w.is_empty())
            .unwrap_or(true)
        {
            return Err(FlussLakeError::InvalidCatalogConfig(format!(
                "missing required `{}` in lake catalog properties",
                CatalogOptions::WAREHOUSE
            )));
        }
        Ok(Self { options })
    }

    /// The Paimon catalog options, ready for `CatalogFactory::create`.
    pub fn options(&self) -> &Options {
        &self.options
    }

    /// Consumes the config, yielding the owned options.
    pub fn into_options(self) -> Options {
        self.options
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn props(pairs: &[(&str, &str)]) -> HashMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    #[test]
    fn builds_options_and_passes_through_keys() {
        let config = LakeCatalogConfig::from_catalog_properties(&props(&[
            ("warehouse", "/tmp/wh"),
            ("metastore", "filesystem"),
            ("s3.access-key-id", "ak"),
        ]))
        .unwrap();
        let opts = config.options();
        assert_eq!(opts.get(CatalogOptions::WAREHOUSE).unwrap(), "/tmp/wh");
        assert_eq!(opts.get("metastore").unwrap(), "filesystem");
        assert_eq!(opts.get("s3.access-key-id").unwrap(), "ak");
    }

    #[test]
    fn rejects_missing_warehouse() {
        let err =
            LakeCatalogConfig::from_catalog_properties(&props(&[("metastore", "filesystem")]))
                .unwrap_err();
        assert!(matches!(err, FlussLakeError::InvalidCatalogConfig(_)));
    }

    #[test]
    fn rejects_empty_warehouse() {
        let err =
            LakeCatalogConfig::from_catalog_properties(&props(&[("warehouse", "")])).unwrap_err();
        assert!(matches!(err, FlussLakeError::InvalidCatalogConfig(_)));
    }
}
