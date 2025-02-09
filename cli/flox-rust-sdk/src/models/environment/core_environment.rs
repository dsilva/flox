use std::fs;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::Command;

use log::debug;
use pollster::FutureExt;
use thiserror::Error;
use tracing::warn;

use super::{
    copy_dir_recursive,
    CanonicalizeError,
    InstallationAttempt,
    UninstallationAttempt,
    UpdateResult,
    LOCKFILE_FILENAME,
    MANIFEST_FILENAME,
};
use crate::data::CanonicalPath;
use crate::flox::Flox;
use crate::models::container_builder::ContainerBuilder;
use crate::models::environment::{call_pkgdb, global_manifest_path};
use crate::models::lockfile::{
    LockedManifest,
    LockedManifestCatalog,
    LockedManifestError,
    LockedManifestPkgdb,
    LockedPackageCatalog,
};
use crate::models::manifest::{
    insert_packages,
    remove_packages,
    Manifest,
    PackageToInstall,
    TomlEditError,
    TypedManifest,
    TypedManifestCatalog,
};
use crate::models::pkgdb::{
    error_codes,
    CallPkgDbError,
    PkgDbError,
    UpgradeResult,
    UpgradeResultJSON,
    PKGDB_BIN,
};
use crate::providers::catalog::{self, ClientTrait};
use crate::utils::CommandExt;

pub struct ReadOnly {}
struct ReadWrite {}

/// A view of an environment directory
/// that can be used to build, link, and edit the environment.
///
/// This is a generic file based implementation that should be
/// used by implementations of [super::Environment].
pub struct CoreEnvironment<State = ReadOnly> {
    /// A generic environment directory containing
    /// `manifest.toml` and optionally `manifest.lock`,
    /// as well as any assets consumed by the environment.
    ///
    /// Commonly /.../.flox/env/
    env_dir: PathBuf,
    _state: State,
}

impl<State> CoreEnvironment<State> {
    /// Get the underlying path to the environment directory
    pub fn path(&self) -> &Path {
        &self.env_dir
    }

    /// Get the manifest file
    pub fn manifest_path(&self) -> PathBuf {
        self.env_dir.join(MANIFEST_FILENAME)
    }

    /// Get the path to the lockfile
    ///
    /// Note: may not exist
    pub fn lockfile_path(&self) -> PathBuf {
        self.env_dir.join(LOCKFILE_FILENAME)
    }

    /// Read the manifest file
    fn manifest_content(&self) -> Result<String, CoreEnvironmentError> {
        fs::read_to_string(self.manifest_path()).map_err(CoreEnvironmentError::OpenManifest)
    }

    /// Lock the environment.
    ///
    /// When a catalog client is provided, the catalog will be used to lock any
    /// "V1" manifest.
    /// Without a catalog client, only "V0" manifests can be locked using the pkgdb.
    /// If a "V1" manifest is locked without a catalog client, an error will be returned.
    ///
    /// This re-writes the lock if it exists.
    ///
    /// Technically this does write to disk as a side effect for now.
    /// It's included in the [ReadOnly] struct for ergonomic reasons
    /// and because it doesn't modify the manifest.
    ///
    /// todo: should we always write the lockfile to disk?
    pub fn lock(&mut self, flox: &Flox) -> Result<LockedManifest, CoreEnvironmentError> {
        let manifest: TypedManifest = toml::from_str(&self.manifest_content()?)
            .map_err(CoreEnvironmentError::DeserializeManifest)?;

        let lockfile = match manifest {
            TypedManifest::Pkgdb(_) => {
                tracing::debug!("using pkgdb to lock");
                LockedManifest::Pkgdb(self.lock_with_pkgdb(flox)?)
            },
            TypedManifest::Catalog(manifest) => {
                let Some(ref client) = flox.catalog_client else {
                    return Err(CoreEnvironmentError::CatalogClientMissing);
                };
                tracing::debug!("using catalog client to lock");
                LockedManifest::Catalog(self.lock_with_catalog_client(client, *manifest)?)
            },
        };

        let environment_lockfile_path = self.lockfile_path();

        // Write the lockfile to disk
        // todo: do we always want to do this?
        debug!(
            "generated lockfile, writing to {}",
            environment_lockfile_path.display()
        );
        std::fs::write(
            &environment_lockfile_path,
            serde_json::to_string_pretty(&lockfile).unwrap(),
        )
        .map_err(CoreEnvironmentError::WriteLockfile)?;

        Ok(lockfile)
    }

    /// Lock the environment with the pkgdb
    ///
    /// Passes the manifest and the existing lockfile to `pkgdb manifest lock`.
    /// The lockfile is used to lock the underlying package registry.
    /// If the environment has no lockfile, the global lockfile is used as a base instead.
    fn lock_with_pkgdb(
        &mut self,
        flox: &Flox,
    ) -> Result<LockedManifestPkgdb, CoreEnvironmentError> {
        let manifest_path = self.manifest_path();
        let environment_lockfile_path = self.lockfile_path();
        let existing_lockfile_path = if environment_lockfile_path.exists() {
            debug!(
                "found existing lockfile: {}",
                environment_lockfile_path.display()
            );
            environment_lockfile_path.clone()
        } else {
            debug!("no existing lockfile found, using the global lockfile as a base");
            // Use the global lock so we're less likely to kick off a pkgdb
            // scrape in e.g. an install.
            LockedManifestPkgdb::ensure_global_lockfile(flox)
                .map_err(CoreEnvironmentError::LockedManifest)?
        };
        let lockfile_path = CanonicalPath::new(existing_lockfile_path)
            .map_err(CoreEnvironmentError::BadLockfilePath)?;

        let lockfile = LockedManifestPkgdb::lock_manifest(
            Path::new(&*PKGDB_BIN),
            &manifest_path,
            &lockfile_path,
            &global_manifest_path(flox),
        )
        .map_err(CoreEnvironmentError::LockedManifest)?;
        Ok(lockfile)
    }

    /// Lock the environment with the catalog client
    ///
    /// If a lockfile exists, it is used as a base.
    /// If the manifest should be locked without a base,
    /// remove the lockfile before calling this function or use [Self::upgrade].
    fn lock_with_catalog_client(
        &self,
        client: &catalog::Client,
        manifest: TypedManifestCatalog,
    ) -> Result<LockedManifestCatalog, CoreEnvironmentError> {
        let existing_lockfile = 'lockfile: {
            let Ok(lockfile_path) = CanonicalPath::new(self.lockfile_path()) else {
                break 'lockfile None;
            };
            let lockfile = LockedManifest::read_from_file(&lockfile_path)
                .map_err(CoreEnvironmentError::LockedManifest)?;
            match lockfile {
                LockedManifest::Catalog(lockfile) => Some(lockfile),
                _ => {
                    warn!(
                        "Found version 1 manifest, but lockfile doesn't match: Ignoring lockfile."
                    );
                    None
                },
            }
        };

        LockedManifestCatalog::lock_manifest(&manifest, existing_lockfile.as_ref(), client)
            .block_on()
            .map_err(CoreEnvironmentError::LockedManifest)
    }

    /// Build the environment.
    ///
    /// Technically this does write to disk as a side effect for now.
    /// It's included in the [ReadOnly] struct for ergonomic reasons
    /// and because it doesn't modify the manifest.
    ///
    /// Does not lock the manifest or link the environment to an out path.
    /// Each should be done explicitly if necessary by the caller
    /// using [Self::lock] and [Self::link]:
    ///
    /// ```no_run
    /// # use flox_rust_sdk::models::environment::CoreEnvironment;
    /// # use flox_rust_sdk::flox::Flox;
    /// let flox: Flox = unimplemented!();
    /// let core_env: CoreEnvironment = unimplemented!();
    ///
    /// core_env.lock(&flox).unwrap();
    /// let store_path = core_env.build(&flox).unwrap();
    /// core_env
    ///     .link(&flox, "/path/to/out-link", &Some(store_path))
    ///     .unwrap();
    /// ```
    #[must_use = "don't discard the store path of built environments"]
    pub fn build(&mut self, flox: &Flox) -> Result<PathBuf, CoreEnvironmentError> {
        let lockfile_path = CanonicalPath::new(self.lockfile_path())
            .map_err(CoreEnvironmentError::BadLockfilePath)?;
        let lockfile = LockedManifest::read_from_file(&lockfile_path)
            .map_err(CoreEnvironmentError::LockedManifest)?;

        debug!(
            "building environment: system={}, lockfilePath={}",
            &flox.system,
            lockfile_path.display()
        );

        let store_path = lockfile
            .build(Path::new(&*PKGDB_BIN), None, &None)
            .map_err(CoreEnvironmentError::LockedManifest)?;

        debug!(
            "built locked environment, store path={}",
            store_path.display()
        );

        Ok(store_path)
    }

    /// Creates a [ContainerBuilder] from the environment.
    ///
    /// The sink is typically a [File](std::fs::File), [Stdout](std::io::Stdout)
    /// but can be any type that implements [Write](std::io::Write).
    ///
    /// While container _images_ can be created on any platform,
    /// only linux _containers_ can be run with `docker` or `podman`.
    /// Building an environment for linux on a non-linux platform (macos),
    /// will likely fail unless all packages in the environment can be substituted.
    ///
    /// There are mitigations for this, such as building within a VM or container.
    /// Such solutions are out of scope at this point.
    /// Until then, this function will error with [CoreEnvironmentError::ContainerizeUnsupportedSystem]
    /// if the environment is not linux.
    ///
    /// [Self::lock]s if necessary.
    ///
    /// Technically this does write to disk as a side effect (i.e. by locking).
    /// It's included in the [ReadOnly] struct for ergonomic reasons
    /// and because it doesn't modify the manifest.
    ///
    /// todo: should we always write the lockfile to disk?
    pub fn build_container(
        &mut self,
        flox: &Flox,
    ) -> Result<ContainerBuilder, CoreEnvironmentError> {
        if std::env::consts::OS != "linux" {
            return Err(CoreEnvironmentError::ContainerizeUnsupportedSystem(
                std::env::consts::OS.to_string(),
            ));
        }

        let lockfile = self.lock(flox)?;

        debug!(
            "building container: system={}, lockfilePath={}",
            &flox.system,
            self.lockfile_path().display()
        );

        let builder = lockfile
            .build_container(Path::new(&*PKGDB_BIN))
            .map_err(CoreEnvironmentError::LockedManifest)?;
        Ok(builder)
    }

    /// Create a new out-link for the environment at the given path.
    /// Optionally a store path to the built environment can be provided,
    /// to avoid building the environment again.
    /// Such a store path can be obtained e.g. from [Self::build].
    ///
    /// Builds the environment if necessary.
    ///
    /// Like [Self::build], this requires the environment to be locked.
    /// This method will _not_ create or update the lockfile.
    ///
    /// Errors if the environment  is not locked or cannot be built.
    ///
    /// TODO: should we always build implicitly?
    pub fn link(
        &mut self,
        flox: &Flox,
        out_link_path: impl AsRef<Path>,
        store_path: &Option<PathBuf>,
    ) -> Result<(), CoreEnvironmentError> {
        let lockfile_path = CanonicalPath::new(self.lockfile_path())
            .map_err(CoreEnvironmentError::BadLockfilePath)?;
        let lockfile = LockedManifest::read_from_file(&lockfile_path)
            .map_err(CoreEnvironmentError::LockedManifest)?;

        debug!(
            "linking environment: system={}, lockfilePath={}, outLinkPath={}",
            &flox.system,
            lockfile_path.display(),
            out_link_path.as_ref().display()
        );

        // Note: when `store_path` is `Some`, `--store-path` is passed to `pkgdb buildenv`
        // which skips the build and only attempts to link the environment.
        lockfile
            .build(
                Path::new(&*PKGDB_BIN),
                Some(out_link_path.as_ref()),
                store_path,
            )
            .map_err(CoreEnvironmentError::LockedManifest)?;
        Ok(())
    }
}

/// Environment modifying methods do not link the new environment to an out path.
/// Linking should be done by the caller.
/// Since files referenced by the environment are ingested into the nix store,
/// the same [CoreEnvironment] instance can be used
/// even if the concrete [super::Environment] tracks the files in a different way
/// such as a git repository or a database.
impl CoreEnvironment<ReadOnly> {
    /// Create a new environment view for the given directory
    ///
    /// This assumes that the directory contains a valid manifest.
    pub fn new(env_dir: impl AsRef<Path>) -> Self {
        CoreEnvironment {
            env_dir: env_dir.as_ref().to_path_buf(),
            _state: ReadOnly {},
        }
    }

    /// Install packages to the environment atomically
    ///
    /// Returns the new manifest content if the environment was modified. Also
    /// returns a map of the packages that were already installed. The installation
    /// will proceed if at least one of the requested packages were added to the
    /// manifest.
    pub fn install(
        &mut self,
        packages: &[PackageToInstall],
        flox: &Flox,
    ) -> Result<InstallationAttempt, CoreEnvironmentError> {
        let current_manifest_contents = self.manifest_content()?;
        let mut installation = insert_packages(&current_manifest_contents, packages)
            .map(|insertion| InstallationAttempt {
                new_manifest: insertion.new_toml.map(|toml| toml.to_string()),
                already_installed: insertion.already_installed,
                store_path: None,
            })
            .map_err(CoreEnvironmentError::ModifyToml)?;
        if let Some(ref new_manifest) = installation.new_manifest {
            let store_path = self.transact_with_manifest_contents(new_manifest, flox)?;
            installation.store_path = Some(store_path);
        }
        Ok(installation)
    }

    /// Uninstall packages from the environment atomically
    ///
    /// Returns true if the environment was modified and false otherwise.
    /// TODO: this should return a list of packages that were actually
    /// uninstalled rather than a bool.
    pub fn uninstall(
        &mut self,
        packages: Vec<String>,
        flox: &Flox,
    ) -> Result<UninstallationAttempt, CoreEnvironmentError> {
        let current_manifest_contents = self.manifest_content()?;
        let toml = remove_packages(&current_manifest_contents, &packages)
            .map_err(CoreEnvironmentError::ModifyToml)?;
        let store_path = self.transact_with_manifest_contents(toml.to_string(), flox)?;
        Ok(UninstallationAttempt {
            new_manifest: Some(toml.to_string()),
            store_path: Some(store_path),
        })
    }

    /// Atomically edit this environment, ensuring that it still builds
    pub fn edit(
        &mut self,
        flox: &Flox,
        contents: String,
    ) -> Result<EditResult, CoreEnvironmentError> {
        let old_contents = self.manifest_content()?;

        // skip the edit if the contents are unchanged
        // note: consumers of this function may call [Self::link] separately,
        //       causing an evaluation/build of the environment.
        if contents == old_contents {
            return Ok(EditResult::Unchanged);
        }

        let store_path = self.transact_with_manifest_contents(&contents, flox)?;

        EditResult::new(&old_contents, &contents, Some(store_path))
    }

    /// Atomically edit this environment, without checking that it still builds
    ///
    /// This is unsafe as it can create broken environments!
    /// Used by the implementation of <https://github.com/flox/flox/issues/823>
    /// and may be removed in the future in favor of something like <https://github.com/flox/flox/pull/681>
    pub(crate) fn edit_unsafe(
        &mut self,
        flox: &Flox,
        contents: String,
    ) -> Result<Result<EditResult, CoreEnvironmentError>, CoreEnvironmentError> {
        let old_contents = self.manifest_content()?;

        // skip the edit if the contents are unchanged
        // note: consumers of this function may call [Self::link] separately,
        //       causing an evaluation/build of the environment.
        if contents == old_contents {
            return Ok(Ok(EditResult::Unchanged));
        }

        let tempdir = tempfile::tempdir_in(&flox.temp_dir)
            .map_err(CoreEnvironmentError::MakeSandbox)?
            .into_path();

        debug!(
            "transaction: making temporary environment in {}",
            tempdir.display()
        );
        let mut temp_env = self.writable(&tempdir)?;

        debug!("transaction: updating manifest");
        temp_env.update_manifest(&contents)?;

        debug!("transaction: building environment, ignoring errors (unsafe)");

        if let Err(lock_err) = temp_env.lock(flox) {
            debug!("transaction: lock failed: {:?}", lock_err);
            debug!("transaction: replacing environment");
            self.replace_with(temp_env)?;
            return Ok(Err(lock_err));
        };

        let build_attempt = temp_env.build(flox);

        debug!("transaction: replacing environment");
        self.replace_with(temp_env)?;

        match build_attempt {
            Ok(store_path) => Ok(EditResult::new(&old_contents, &contents, Some(store_path))),
            Err(err) => Ok(Err(err)),
        }
    }

    /// Update the inputs of an environment atomically.
    pub fn update(
        &mut self,
        flox: &Flox,
        inputs: Vec<String>,
    ) -> Result<UpdateResult, CoreEnvironmentError> {
        // TODO: double check canonicalization
        let UpdateResult {
            new_lockfile,
            old_lockfile,
            ..
        } = LockedManifestPkgdb::update_manifest(
            flox,
            Some(self.manifest_path()),
            self.lockfile_path(),
            inputs,
        )
        .map_err(CoreEnvironmentError::LockedManifest)?;

        let store_path = self.transact_with_lockfile_contents(
            serde_json::to_string_pretty(&new_lockfile).unwrap(),
            flox,
        )?;

        Ok(UpdateResult {
            new_lockfile,
            old_lockfile,
            store_path: Some(store_path),
        })
    }

    /// Atomically upgrade packages in this environment
    ///
    /// First resolve a new lockfile with upgraded packages using either pkgdb or the catalog client.
    /// Then verify the new lockfile by building the environment.
    /// Finally replace the existing environment with the new, upgraded one.
    pub fn upgrade(
        &mut self,
        flox: &Flox,
        groups_or_iids: &[String],
    ) -> Result<UpgradeResult, CoreEnvironmentError> {
        let manifest = toml::from_str(&self.manifest_content()?)
            .map_err(CoreEnvironmentError::DeserializeManifest)?;

        let (lockfile, upgraded) = match manifest {
            TypedManifest::Pkgdb(_) => {
                let (lockfile, upgraded) = self.upgrade_with_pkgdb(flox, groups_or_iids)?;
                (LockedManifest::Pkgdb(lockfile), upgraded)
            },
            TypedManifest::Catalog(catalog) => {
                let client = flox
                    .catalog_client
                    .as_ref()
                    .ok_or(CoreEnvironmentError::CatalogClientMissing)?;

                let (lockfile, upgraded) =
                    self.upgrade_with_catalog_client(client, groups_or_iids, &catalog)?;

                let upgraded = upgraded
                    .into_iter()
                    .map(|(_, pkg)| pkg.install_id.clone())
                    .collect();

                (LockedManifest::Catalog(lockfile), upgraded)
            },
        };

        let store_path =
            self.transact_with_lockfile_contents(serde_json::json!(&lockfile).to_string(), flox)?;

        Ok(UpgradeResult {
            packages: upgraded,
            store_path: Some(store_path),
        })
    }

    fn upgrade_with_pkgdb(
        &mut self,
        flox: &Flox,
        groups_or_iids: &[String],
    ) -> Result<(LockedManifestPkgdb, Vec<String>), CoreEnvironmentError> {
        let manifest_path = self.manifest_path();
        let lockfile_path = self.lockfile_path();
        let maybe_lockfile = if lockfile_path.exists() {
            debug!("found existing lockfile: {}", lockfile_path.display());
            Some(lockfile_path)
        } else {
            debug!("no existing lockfile found");
            None
        };
        let mut pkgdb_cmd = Command::new(Path::new(&*PKGDB_BIN));
        pkgdb_cmd
            .args(["manifest", "upgrade"])
            .arg("--ga-registry")
            .arg("--global-manifest")
            .arg(global_manifest_path(flox))
            .arg("--manifest")
            .arg(manifest_path);
        if let Some(lf_path) = maybe_lockfile {
            let canonical_lockfile_path =
                CanonicalPath::new(lf_path).map_err(CoreEnvironmentError::BadLockfilePath)?;
            pkgdb_cmd.arg("--lockfile").arg(canonical_lockfile_path);
        }
        pkgdb_cmd.args(groups_or_iids);

        debug!(
            "upgrading environment with command: {}",
            pkgdb_cmd.display()
        );
        let json: UpgradeResultJSON = serde_json::from_value(
            call_pkgdb(pkgdb_cmd).map_err(CoreEnvironmentError::UpgradeFailed)?,
        )
        .map_err(CoreEnvironmentError::ParseUpgradeOutput)?;

        Ok((json.lockfile, json.result.0))
    }

    /// Upgrade the given groups or install ids in the environment using the catalog client.
    /// The environment is upgraded by locking the existing manifest
    /// using [LockedManifestCatalog::lock_manifest] with the existing lockfile as a seed,
    /// where the upgraded packages have been filtered out causing them to be re-resolved.
    fn upgrade_with_catalog_client(
        &mut self,
        client: &impl ClientTrait,
        groups_or_iids: &[String],
        manifest: &TypedManifestCatalog,
    ) -> Result<
        (
            LockedManifestCatalog,
            Vec<(LockedPackageCatalog, LockedPackageCatalog)>,
        ),
        CoreEnvironmentError,
    > {
        let existing_lockfile = 'lockfile: {
            let Ok(lockfile_path) = CanonicalPath::new(self.lockfile_path()) else {
                break 'lockfile None;
            };
            let lockfile = LockedManifest::read_from_file(&lockfile_path)
                .map_err(CoreEnvironmentError::LockedManifest)?;
            match lockfile {
                LockedManifest::Catalog(lockfile) => Some(lockfile),
                _ => {
                    warn!(
                        "Found version 1 manifest, but lockfile doesn't match: Ignoring lockfile."
                    );
                    None
                },
            }
        };

        let previous_packages = existing_lockfile
            .as_ref()
            .map(|lockfile| lockfile.packages.clone())
            .unwrap_or_default();

        // Create a seed lockfile by "unlocking" (i.e. removing the locked entries of)
        // all packages matching the given groups or iids.
        // If no groups or iids are provided, all packages are unlocked.
        let seed_lockfile = if groups_or_iids.is_empty() {
            debug!("no groups or iids provided, unlocking all packages");
            None
        } else {
            existing_lockfile.map(|mut lockfile| {
                lockfile.unlock_packages_by_group_or_iid(groups_or_iids);
                lockfile
            })
        };

        let upgraded =
            LockedManifestCatalog::lock_manifest(manifest, seed_lockfile.as_ref(), client)
                .block_on()
                .map_err(CoreEnvironmentError::LockedManifest)?;

        // find all packages that after upgrading have a different derivation
        let package_diff = upgraded
            .packages
            .iter()
            .filter_map(move |pkg| {
                previous_packages
                    .iter()
                    .find(|prev| {
                        prev.install_id == pkg.install_id && prev.derivation != pkg.derivation
                    })
                    .map(|prev| (prev.clone(), pkg.clone()))
            })
            .collect();

        Ok((upgraded, package_diff))
    }

    /// Makes a temporary copy of the environment so modifications to the manifest
    /// can be applied without modifying the original environment.
    fn writable(
        &mut self,
        tempdir: impl AsRef<Path>,
    ) -> Result<CoreEnvironment<ReadWrite>, CoreEnvironmentError> {
        copy_dir_recursive(&self.env_dir, &tempdir.as_ref(), true)
            .map_err(CoreEnvironmentError::MakeTemporaryEnv)?;

        Ok(CoreEnvironment {
            env_dir: tempdir.as_ref().to_path_buf(),
            _state: ReadWrite {},
        })
    }

    /// Replace the contents of this environment (e.g. `.flox/env`)
    /// with that of another environment.
    ///
    /// This will **not** set any out-links to updated versions of the environment.
    fn replace_with(
        &mut self,
        replacement: CoreEnvironment<ReadWrite>,
    ) -> Result<(), CoreEnvironmentError> {
        let transaction_backup = self.env_dir.with_extension("tmp");

        if transaction_backup.exists() {
            debug!(
                "transaction backup exists: {}",
                transaction_backup.display()
            );
            return Err(CoreEnvironmentError::PriorTransaction(transaction_backup));
        }
        debug!(
            "backing up env: from={}, to={}",
            self.env_dir.display(),
            transaction_backup.display()
        );
        fs::rename(&self.env_dir, &transaction_backup)
            .map_err(CoreEnvironmentError::BackupTransaction)?;
        // try to restore the backup if the move fails
        debug!(
            "replacing original env: from={}, to={}",
            replacement.env_dir.display(),
            self.env_dir.display()
        );
        if let Err(err) = copy_dir_recursive(&replacement.env_dir, &self.env_dir, true) {
            debug!(
                "failed to replace env ({}), restoring backup: from={}, to={}",
                err,
                transaction_backup.display(),
                self.env_dir.display(),
            );
            fs::remove_dir_all(&self.env_dir).map_err(CoreEnvironmentError::AbortTransaction)?;
            fs::rename(transaction_backup, &self.env_dir)
                .map_err(CoreEnvironmentError::AbortTransaction)?;
            return Err(CoreEnvironmentError::Move(err));
        }
        debug!("removing backup: path={}", transaction_backup.display());
        fs::remove_dir_all(transaction_backup).map_err(CoreEnvironmentError::RemoveBackup)?;
        Ok(())
    }

    /// Attempt to transactionally replace the manifest contents
    #[must_use = "don't discard the store path of built environments"]
    fn transact_with_manifest_contents(
        &mut self,
        manifest_contents: impl AsRef<str>,
        flox: &Flox,
    ) -> Result<PathBuf, CoreEnvironmentError> {
        let tempdir = tempfile::tempdir_in(&flox.temp_dir)
            .map_err(CoreEnvironmentError::MakeSandbox)?
            .into_path();

        debug!(
            "transaction: making temporary environment in {}",
            tempdir.display()
        );
        let mut temp_env = self.writable(&tempdir)?;

        debug!("transaction: updating manifest");
        temp_env.update_manifest(&manifest_contents)?;

        debug!("transaction: locking environment");
        temp_env.lock(flox)?;

        debug!("transaction: building environment");
        let store_path = temp_env.build(flox)?;

        debug!("transaction: replacing environment");
        self.replace_with(temp_env)?;
        Ok(store_path)
    }

    /// Attempt to transactionally replace the lockfile contents
    ///
    /// The lockfile_contents passed to this function must be generated by pkgdb
    /// so that calling `pkgdb manifest lock` with the new lockfile_contents is
    /// idempotent.
    ///
    /// TODO: this is separate from transact_with_manifest_contents because it
    /// shouldn't have to call lock. Currently build calls lock, but we
    /// shouldn't have to lock a second time.
    #[must_use = "don't discard the store path of built environments"]
    fn transact_with_lockfile_contents(
        &mut self,
        lockfile_contents: impl AsRef<str>,
        flox: &Flox,
    ) -> Result<PathBuf, CoreEnvironmentError> {
        let tempdir = tempfile::tempdir_in(&flox.temp_dir)
            .map_err(CoreEnvironmentError::MakeSandbox)?
            .into_path();

        debug!(
            "transaction: making temporary environment in {}",
            tempdir.display()
        );
        let mut temp_env = self.writable(&tempdir)?;

        debug!("transaction: updating lockfile");
        temp_env.update_lockfile(&lockfile_contents)?;

        debug!("transaction: building environment");
        let store_path = temp_env.build(flox)?;

        debug!("transaction: replacing environment");
        self.replace_with(temp_env)?;
        Ok(store_path)
    }
}

/// A writable view of an environment directory
///
/// Typically within a temporary directory created by [CoreEnvironment::writable].
/// This is not public to enforce that environments are only edited atomically.
impl CoreEnvironment<ReadWrite> {
    /// Updates the environment manifest with the provided contents
    fn update_manifest(&mut self, contents: impl AsRef<str>) -> Result<(), CoreEnvironmentError> {
        debug!("writing new manifest to {}", self.manifest_path().display());
        let mut manifest_file = std::fs::OpenOptions::new()
            .write(true)
            .truncate(true)
            .open(self.manifest_path())
            .map_err(CoreEnvironmentError::OpenManifest)?;
        manifest_file
            .write_all(contents.as_ref().as_bytes())
            .map_err(CoreEnvironmentError::UpdateManifest)?;
        Ok(())
    }

    /// Updates the environment lockfile with the provided contents
    fn update_lockfile(&mut self, contents: impl AsRef<str>) -> Result<(), CoreEnvironmentError> {
        debug!("writing lockfile to {}", self.lockfile_path().display());
        std::fs::write(self.lockfile_path(), contents.as_ref())
            .map_err(CoreEnvironmentError::WriteLockfile)?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EditResult {
    /// The manifest was not modified.
    Unchanged,
    /// The manifest was modified, and the user needs to re-activate it.
    ReActivateRequired { store_path: Option<PathBuf> },
    /// The manifest was modified, but the user does not need to re-activate it.
    Success { store_path: Option<PathBuf> },
}

impl EditResult {
    pub fn new(
        old_manifest: &str,
        new_manifest: &str,
        store_path: Option<PathBuf>,
    ) -> Result<Self, CoreEnvironmentError> {
        if old_manifest == new_manifest {
            Ok(Self::Unchanged)
        } else {
            // todo: use a single toml crate (toml_edit already implements serde traits)
            // TODO: use different error variants, users _can_ fix errors in the _new_ manifest
            //       but they _can't_ fix errors in the _old_ manifest
            let old_manifest: Manifest =
                toml::from_str(old_manifest).map_err(CoreEnvironmentError::DeserializeManifest)?;
            let new_manifest: Manifest =
                toml::from_str(new_manifest).map_err(CoreEnvironmentError::DeserializeManifest)?;
            // TODO: some modifications to `install` currently require re-activation
            if old_manifest.hook != new_manifest.hook || old_manifest.vars != new_manifest.vars {
                Ok(Self::ReActivateRequired { store_path })
            } else {
                Ok(Self::Success { store_path })
            }
        }
    }

    pub fn store_path(&self) -> Option<PathBuf> {
        match self {
            EditResult::Unchanged => None,
            EditResult::ReActivateRequired { store_path } => store_path.clone(),
            EditResult::Success { store_path } => store_path.clone(),
        }
    }
}

#[derive(Debug, Error)]
pub enum CoreEnvironmentError {
    // region: immutable manifest errors
    #[error("could not modify manifest")]
    ModifyToml(#[source] TomlEditError),
    #[error("could not deserialize manifest")]
    DeserializeManifest(#[source] toml::de::Error),
    // endregion

    // region: transaction errors
    #[error("could not make temporary directory for transaction")]
    MakeSandbox(#[source] std::io::Error),

    #[error("couldn't write new lockfile contents")]
    WriteLockfile(#[source] std::io::Error),

    #[error("could not make temporary copy of environment")]
    MakeTemporaryEnv(#[source] std::io::Error),
    /// Thrown when a .flox/env.tmp directory already exists
    #[error("prior transaction in progress -- delete {0} to discard")]
    PriorTransaction(PathBuf),
    #[error("could not create backup for transaction")]
    BackupTransaction(#[source] std::io::Error),
    #[error("Failed to abort transaction; backup could not be moved back into place")]
    AbortTransaction(#[source] std::io::Error),
    #[error("Failed to move modified environment into place")]
    Move(#[source] std::io::Error),
    #[error("Failed to remove transaction backup")]
    RemoveBackup(#[source] std::io::Error),

    // endregion

    // region: mutable manifest errors
    #[error("could not open manifest")]
    OpenManifest(#[source] std::io::Error),
    #[error("could not write manifest")]
    UpdateManifest(#[source] std::io::Error),
    // endregion

    // region: pkgdb manifest errors
    #[error(transparent)]
    LockedManifest(LockedManifestError),

    #[error(transparent)]
    BadLockfilePath(CanonicalizeError),

    // todo: refactor upgrade to use `LockedManifest`
    #[error("unexpected output from pkgdb upgrade")]
    ParseUpgradeOutput(#[source] serde_json::Error),
    #[error("failed to upgrade environment")]
    UpgradeFailed(#[source] CallPkgDbError),
    // endregion

    // endregion
    #[error("unsupported system to build container: {0}")]
    ContainerizeUnsupportedSystem(String),

    #[error("Could not process catalog manifest without a catalog client")]
    CatalogClientMissing,
}

impl CoreEnvironmentError {
    pub fn is_incompatible_system_error(&self) -> bool {
        matches!(
            self,
            CoreEnvironmentError::LockedManifest(LockedManifestError::BuildEnv(
                CallPkgDbError::PkgDbError(PkgDbError {
                    exit_code: error_codes::LOCKFILE_INCOMPATIBLE_SYSTEM,
                    ..
                })
            ))
        )
    }

    pub fn is_incompatible_package_error(&self) -> bool {
        #[allow(clippy::match_like_matches_macro)] // rustfmt can't handle this as a match!
        match self.pkgdb_exit_code() {
            Some(exit_code)
                if [
                    error_codes::PACKAGE_BUILD_FAILURE,
                    error_codes::PACKAGE_EVAL_FAILURE,
                    error_codes::PACKAGE_EVAL_INCOMPATIBLE_SYSTEM,
                ]
                .contains(exit_code) =>
            {
                true
            },
            _ => false,
        }
    }

    /// If the error contains a PkgDbError with an exit_code, return it.
    /// Otherwise return None.
    pub fn pkgdb_exit_code(&self) -> Option<&u64> {
        match self {
            CoreEnvironmentError::LockedManifest(LockedManifestError::BuildEnv(
                CallPkgDbError::PkgDbError(PkgDbError { exit_code, .. }),
            )) => Some(exit_code),
            _ => None,
        }
    }
}

pub mod test_helpers {
    use indoc::indoc;

    use super::*;
    use crate::flox::Flox;

    #[cfg(target_os = "macos")]
    pub const MANIFEST_INCOMPATIBLE_SYSTEM: &str = indoc! {r#"
        [options]
        systems = ["x86_64-linux"]
        "#};

    #[cfg(target_os = "linux")]
    pub const MANIFEST_INCOMPATIBLE_SYSTEM: &str = indoc! {r#"
        [options]
        systems = ["aarch64-darwin"]
        "#};

    pub fn new_core_environment(flox: &Flox, contents: &str) -> CoreEnvironment {
        let env_path = tempfile::tempdir_in(&flox.temp_dir).unwrap().into_path();
        fs::write(env_path.join(MANIFEST_FILENAME), contents).unwrap();

        CoreEnvironment::new(&env_path)
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use catalog_api_v1::types::ResolvedPackageDescriptor;
    use chrono::{DateTime, Utc};
    use indoc::{formatdoc, indoc};
    use serial_test::serial;
    use tempfile::{tempdir_in, TempDir};
    use tests::test_helpers::MANIFEST_INCOMPATIBLE_SYSTEM;

    use self::catalog::{CatalogPage, MockClient, ResolvedPackageGroup};
    use self::test_helpers::new_core_environment;
    use super::*;
    use crate::data::Version;
    use crate::flox::test_helpers::{flox_instance, flox_instance_with_global_lock};
    use crate::models::manifest::DEFAULT_GROUP_NAME;
    use crate::models::{lockfile, manifest};

    /// Create a CoreEnvironment with an empty manifest
    ///
    /// This calls flox_instance_with_global_lock(),
    /// so the resulting environment can be built without incurring a pkgdb scrape.
    fn empty_core_environment() -> (CoreEnvironment, Flox, TempDir) {
        let (flox, tempdir) = flox_instance_with_global_lock();

        (new_core_environment(&flox, ""), flox, tempdir)
    }

    /// Check that `edit` updates the manifest and creates a lockfile
    #[test]
    #[serial]
    #[cfg(feature = "impure-unit-tests")]
    fn edit_env_creates_manifest_and_lockfile() {
        let (flox, tempdir) = flox_instance_with_global_lock();

        let env_path = tempfile::tempdir_in(&tempdir).unwrap();
        fs::write(env_path.path().join(MANIFEST_FILENAME), "").unwrap();

        let mut env_view = CoreEnvironment::new(&env_path);

        let new_env_str = r#"
        [install]
        hello = {}
        "#;

        env_view.edit(&flox, new_env_str.to_string()).unwrap();

        assert_eq!(env_view.manifest_content().unwrap(), new_env_str);
        assert!(env_view.env_dir.join(LOCKFILE_FILENAME).exists());
    }

    /// A no-op with edit returns EditResult::Unchanged
    #[test]
    #[serial]
    fn edit_no_op_returns_unchanged() {
        let (mut env_view, flox, _temp_dir_handle) = empty_core_environment();

        let result = env_view.edit(&flox, "".to_string()).unwrap();

        assert!(matches!(result, EditResult::Unchanged));
    }

    /// Trying to build a manifest with a system other than the current one
    /// results in an error that is_incompatible_system_error()
    #[test]
    #[serial]
    fn build_incompatible_system() {
        let (mut env_view, flox, _temp_dir_handle) = empty_core_environment();
        let mut temp_env = env_view
            .writable(tempdir_in(&flox.temp_dir).unwrap().into_path())
            .unwrap();
        temp_env
            .update_manifest(MANIFEST_INCOMPATIBLE_SYSTEM)
            .unwrap();
        temp_env.lock(&flox).unwrap();
        env_view.replace_with(temp_env).unwrap();

        let result = env_view.build(&flox).unwrap_err();

        assert!(result.is_incompatible_system_error());
    }

    /// Trying to build a manifest with a package that is incompatible with the current system
    /// results in an error that is_incompatible_package_error()
    #[test]
    #[serial]
    fn build_incompatible_package() {
        #[cfg(all(target_os = "macos", target_arch = "aarch64"))]
        let manifest_contents = formatdoc! {r#"
        [install]
        glibc.pkg-path = "glibc"

        [options]
        systems = ["aarch64-darwin"]
        "#};

        #[cfg(all(target_os = "macos", target_arch = "x86_64"))]
        let manifest_contents = formatdoc! {r#"
        [install]
        glibc.pkg-path = "glibc"

        [options]
        systems = ["x86_64-darwin"]
        "#};

        #[cfg(all(target_os = "linux", target_arch = "x86_64"))]
        let manifest_contents = formatdoc! {r#"
        [install]
        ps.pkg-path = "darwin.ps"

        [options]
        systems = ["x86_64-linux"]
        "#};

        #[cfg(all(target_os = "linux", target_arch = "aarch64"))]
        let manifest_contents = formatdoc! {r#"
        [install]
        ps.pkg-path = "darwin.ps"

        [options]
        systems = ["aarch64-linux"]
        "#};

        let (mut env_view, flox, _temp_dir_handle) = empty_core_environment();
        let mut temp_env = env_view
            .writable(tempdir_in(&flox.temp_dir).unwrap().into_path())
            .unwrap();
        temp_env.update_manifest(&manifest_contents).unwrap();
        temp_env.lock(&flox).unwrap();
        env_view.replace_with(temp_env).unwrap();

        let result = env_view.build(&flox).unwrap_err();

        assert!(result.is_incompatible_package_error());
    }

    /// Trying to build a manifest with an insecure package results in an error
    /// that is_incompatible_package_error()
    #[test]
    #[serial]
    fn build_insecure_package() {
        let manifest_content = indoc! {r#"
            [install]
            python2.pkg-path = "python2"
            "#
        };
        let (mut env_view, flox, _temp_dir_handle) = empty_core_environment();
        let mut temp_env = env_view
            .writable(tempdir_in(&flox.temp_dir).unwrap().into_path())
            .unwrap();
        temp_env.update_manifest(manifest_content).unwrap();
        temp_env.lock(&flox).unwrap();
        env_view.replace_with(temp_env).unwrap();

        let result = env_view.build(&flox).unwrap_err();

        assert!(result.is_incompatible_package_error());
    }

    /// Installing hello with edit returns EditResult::Success
    #[test]
    #[serial]
    fn edit_adding_package_returns_success() {
        let (mut env_view, flox, _temp_dir_handle) = empty_core_environment();

        let new_env_str = r#"
        [install]
        hello = {}
        "#;

        let result = env_view.edit(&flox, new_env_str.to_string()).unwrap();

        assert!(matches!(result, EditResult::Success { store_path: _ }));
    }

    /// Adding a hook with edit returns EditResult::ReActivateRequired
    #[test]
    #[serial]
    fn edit_adding_hook_returns_re_activate_required() {
        let (mut env_view, flox, _temp_dir_handle) = empty_core_environment();

        let new_env_str = r#"
        [hook]
        on-activate = ""
        "#;

        let result = env_view.edit(&flox, new_env_str.to_string()).unwrap();

        assert!(matches!(result, EditResult::ReActivateRequired {
            store_path: _
        }));
    }

    #[test]
    fn locking_of_v1_manifest_requires_catalog_client() {
        let (mut env_view, mut flox, _temp_dir_handle) = empty_core_environment();
        flox.catalog_client = None;

        fs::write(env_view.manifest_path(), r#"version = 1"#).unwrap();

        let err = env_view
            .lock(&flox)
            .expect_err("should fail to lock v1 lockfile with pkgdb");

        assert!(matches!(err, CoreEnvironmentError::CatalogClientMissing));

        let mut mock_client = MockClient::new(None::<&str>).unwrap();
        mock_client.push_resolve_response(vec![]);
        flox.catalog_client = Option::Some(mock_client.into());

        env_view
            .lock(&flox)
            .expect("lock should succeed with catalog client");
    }

    #[test]
    fn upgrade_with_catalog_client_requires_catalog_client() {
        // flox already has a catalog client
        let (mut env_view, mut flox, _temp_dir_handle) = empty_core_environment();
        fs::write(env_view.manifest_path(), r#"version = 1"#).unwrap();

        flox.catalog_client = None;
        let err = env_view
            .upgrade(&flox, &[])
            .expect_err("upgrade of v1 manifest should fail without client");

        assert!(matches!(err, CoreEnvironmentError::CatalogClientMissing));

        let mut mock_client = MockClient::new(None::<&str>).unwrap();
        mock_client.push_resolve_response(vec![]);
        flox.catalog_client = Option::Some(mock_client.into());
        env_view
            .upgrade(&flox, &[])
            .expect("upgrade should succeed with catalog client");
    }

    /// Check that with an empty list of packages to upgrade, all packages are upgraded
    // TODO: add fixtures for resolve mocks if we add more of these tests
    #[test]
    fn upgrade_with_empty_list_upgrades_all() {
        let (mut env_view, _flox, _temp_dir_handle) = empty_core_environment();

        let mut manifest = manifest::test::empty_catalog_manifest();
        let (foo_iid, foo_descriptor, foo_locked) = lockfile::tests::fake_package("foo", None);
        manifest.install.insert(foo_iid.clone(), foo_descriptor);
        let lockfile = lockfile::LockedManifestCatalog {
            version: Version,
            packages: vec![foo_locked.clone()],
            manifest: manifest.clone(),
        };

        let lockfile_str = serde_json::to_string_pretty(&lockfile).unwrap();

        fs::write(env_view.lockfile_path(), lockfile_str).unwrap();

        let mut mock_client = MockClient::new(None::<&str>).unwrap();
        mock_client.push_resolve_response(vec![ResolvedPackageGroup {
            name: DEFAULT_GROUP_NAME.to_string(),
            pages: vec![CatalogPage {
                packages: Some(vec![ResolvedPackageDescriptor {
                    attr_path: "foo".to_string(),
                    broken: false,
                    derivation: "new derivation".to_string(),
                    description: Some("description".to_string()),
                    install_id: foo_iid.clone(),
                    license: None,
                    locked_url: "locked-url".to_string(),
                    name: "foo".to_string(),
                    outputs: None,
                    outputs_to_install: None,
                    pname: "foo".to_string(),
                    rev: "rev".to_string(),
                    rev_count: 42,
                    rev_date: DateTime::<Utc>::MIN_UTC,
                    scrape_date: DateTime::<Utc>::MIN_UTC,
                    stabilities: None,
                    unfree: None,
                    version: "1.0".to_string(),
                }]),
                page: 1,
                url: "url".to_string(),
            }],
            system: "system".to_string(),
        }]);

        let (_, upgraded_packages) = env_view
            .upgrade_with_catalog_client(&mock_client, &[], &manifest)
            .unwrap();

        assert!(upgraded_packages.len() == 1);
    }

    /// replacing an environment should fail if a backup exists
    #[test]
    fn detects_existing_backup() {
        let (_flox, tempdir) = flox_instance();

        let env_path = tempfile::tempdir_in(&tempdir).unwrap();
        let sandbox_path = tempfile::tempdir_in(&tempdir).unwrap();
        fs::create_dir(env_path.path().with_extension("tmp")).unwrap();

        let mut env_view = CoreEnvironment::new(&env_path);
        let temp_env = env_view.writable(&sandbox_path).unwrap();

        let err = env_view
            .replace_with(temp_env)
            .expect_err("Should fail if backup exists");

        assert!(matches!(err, CoreEnvironmentError::PriorTransaction(_)));
    }

    /// creating backup should fail if env is readonly
    #[test]
    #[ignore = "On Ubuntu github runners this moving a read only directory succeeds.
        thread 'models::environment::core_environment::tests::fails_to_create_backup' panicked at 'Should fail to create backup: dir is readonly: 40555: ()'"]
    fn fails_to_create_backup() {
        let (_flox, tempdir) = flox_instance();

        let env_path = tempfile::tempdir_in(&tempdir).unwrap();
        let sandbox_path = tempfile::tempdir_in(&tempdir).unwrap();

        let mut env_path_permissions = fs::metadata(env_path.path()).unwrap().permissions();
        env_path_permissions.set_readonly(true);

        // force fail by setting dir readonly
        fs::set_permissions(&env_path, env_path_permissions.clone()).unwrap();

        let mut env_view = CoreEnvironment::new(&env_path);
        let temp_env = env_view.writable(&sandbox_path).unwrap();

        let err = env_view.replace_with(temp_env).expect_err(&format!(
            "Should fail to create backup: dir is readonly: {:o}",
            env_path_permissions.mode()
        ));

        assert!(
            matches!(err, CoreEnvironmentError::BackupTransaction(err) if err.kind() == std::io::ErrorKind::PermissionDenied)
        );
    }

    /// linking an environment should set a gc-root
    #[test]
    #[serial]
    #[cfg(feature = "impure-unit-tests")]
    fn build_flox_environment_and_links() {
        let (flox, tempdir) = flox_instance_with_global_lock();

        let env_path = tempfile::tempdir_in(&tempdir).unwrap();
        fs::write(
            env_path.path().join(MANIFEST_FILENAME),
            "
        [install]
        hello = {}
        ",
        )
        .unwrap();

        let mut env_view = CoreEnvironment::new(&env_path);

        env_view.lock(&flox).expect("locking should succeed");
        env_view.build(&flox).expect("build should succeed");
        env_view
            .link(&flox, env_path.path().with_extension("out-link"), &None)
            .expect("link should succeed");

        // very rudimentary check that the environment manifest built correctly
        // and linked to the out-link.
        assert!(env_path
            .path()
            .with_extension("out-link")
            .join("bin/hello")
            .exists());
    }
}
