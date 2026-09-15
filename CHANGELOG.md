# CHANGELOG

<!-- version list -->

## v0.2.0 (2026-09-15)

### Features

- Label-based connection limits with caller-driven takeover
  ([`aa5200b`](https://github.com/aambekar234/fastapi-ws-manager/commit/aa5200b8a6af252f889fb4b2ad7026c19751f0c8))

### Breaking Changes

- `WebSocketManagerConfig.auth_close_code`, `WebSocketManagerConfig.busy_close_code` and
  `WebSocketManagerConfig.client_timeout_close_code` are removed. The codes they configured are now
  the fixed constants `WS_AUTH_FAILED` (1008), `WS_SERVER_BUSY` (1013) and `WS_CLIENT_TIMEOUT`
  (4408), exported from the package. There is no replacement setting.


## v0.1.1 (2026-07-21)

### Bug Fixes

- Ship py.typed marker and keep connections alive when liveness is disabled
  ([`81e33fd`](https://github.com/aambekar234/fastapi-ws-manager/commit/81e33fd23d1a09d1f347f915aae8ad93f9b01f40))


## v0.1.0 (2026-06-17)

### Features

- Prod release
  ([`d6f2312`](https://github.com/aambekar234/fastapi-ws-manager/commit/d6f23121da87c619ea7246b345b7a09ef3b6c650))


## v0.0.0 (2026-06-17)

- Initial Release
