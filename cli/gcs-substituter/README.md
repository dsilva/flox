# gcs-substituter

This is a Nix binary cache substituter that fetches artifacts from Google Cloud Storage (GCS).

## Usage

Example usage:

```bash
gcs-substituter --bucket your-bucket --fallback https://cache.nixos.org --fill-missing --missing-objects-filename missing-objects.txt
```

See `gcs-substituter --help` for details.

## Credentials

This program uses the [google-cloud-auth](https://github.com/yoshidan/google-cloud-rust/blob/main/foundation/auth/README.md) crate,
which reads the GOOGLE_APPLICATION_CREDENTIALS environment variable
and in its absence uses Google Cloud application default credentials.

## Fallback

When items are not found in the GCS bucket, this substituter can fall back to another one.
Using `https://cache.nixos.org` as the fallback (for example) allows access to the public Nix binary cache
while also prioritizing your own GCS bucket.

This is similar to configuring Nix to use multiple binary caches,
but moves control to your caching proxy instead of Nix clients.

Using a fallback here is also a prerequisite for running in fill mode (see below).

## Fill mode

If it does fall back to another substituter, it can also optionally upload the artifact to the GCS bucket.

Use this fill mode to populate your GCS bucket before using it in a locked down environment without bucket write access
and without broader network access (ie, no access to the fallback substituter).

## TODO

- Tests
- More documentation
- Integrating into Flox to start this automatically
- Updating Flox to allow a localhost server as a trusted substituter for Nix