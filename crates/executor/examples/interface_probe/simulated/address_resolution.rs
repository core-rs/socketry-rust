// Released under the MIT License.
// Copyright, 2026, by Samuel Williams.

use super::Scheduler;
use socketry_executor::{
    AddressResolution, AddressResolveOptions, Cancellation, OperationResult, SocketDomain,
};
use std::{
    io,
    net::{IpAddr, SocketAddr},
};

impl Scheduler {
    /// Configure exact-name results, retaining their order and duplicates. This table intentionally does not model DNS search paths, TTLs, or hostname normalization.
    pub fn insert_addresses(&self, name: &str, addresses: Vec<IpAddr>) {
        self.names
            .lock()
            .unwrap()
            .insert(name.to_owned(), addresses);
    }
}

impl AddressResolution for Scheduler {
    async fn address_resolve(
        &self,
        name: &str,
        options: AddressResolveOptions,
        cancellation: &Cancellation,
    ) -> OperationResult<Vec<SocketAddr>> {
        cancellation.check()?;
        if name.is_empty() || name.contains('\0') {
            return Err(io::Error::from(io::ErrorKind::InvalidInput).into());
        }
        let addresses = match name.parse::<IpAddr>() {
            Ok(address) => vec![address],
            Err(_) => self
                .names
                .lock()
                .unwrap()
                .get(name)
                .cloned()
                .ok_or_else(|| io::Error::from(io::ErrorKind::NotFound))?,
        };
        Ok(addresses
            .into_iter()
            .map(|address| SocketAddr::new(address, options.port))
            .filter(|address| {
                options
                    .domain
                    .is_none_or(|domain| SocketDomain::from(*address) == domain)
            })
            .collect())
    }
}
