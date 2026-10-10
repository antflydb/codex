// TEMPORARY, pre-existing and unrelated to thread/state persistence: see
// `codex-core`'s `lib.rs` for the same `recursion_limit` workaround and why.
#![recursion_limit = "256"]

use std::sync::Arc;
use std::sync::Weak;

use codex_core::ThreadManager;
use codex_core::config::Config;
use codex_extension_api::ExtensionRegistryBuilder;
use codex_login::AuthManager;

mod async_scorer;
mod sync_reviewer;

pub use sync_reviewer::install as install_reviewer;

/// Installs the guardian contributors into the extension registry.
pub fn install(
    registry: &mut ExtensionRegistryBuilder<Config>,
    auth_manager: Arc<AuthManager>,
    thread_manager: Weak<ThreadManager>,
) {
    async_scorer::install(registry, auth_manager, thread_manager.clone());
    install_reviewer(registry, thread_manager);
}
