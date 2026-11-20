# Security Policy

Report vulnerabilities privately to the project maintainer before public disclosure.

## Trust boundary
The default pickle serializer is unsafe for untrusted cache files. Only use cache directories writable by the current trusted user. Do not point ninelives at a shared directory where untrusted users can create or replace entries. Optional `hmac_key` signing authenticates cache entries and checkpoint segments before deserialization but does not protect the signing key; store it separately and do not treat it as authorization for shared cache directories.

Cache filenames are derived from digests. Cache directory ownership is checked on POSIX and permissions are restricted to owner access where supported.
