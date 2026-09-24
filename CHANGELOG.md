# Changelog

## [0.14.2](https://github.com/evil8io/drover/compare/v0.14.1...v0.14.2) (2026-09-24)


### Bug Fixes

* **api-filter:** relay a websocket watch in the framing of the upstream ([#74](https://github.com/evil8io/drover/issues/74)) ([7a1b379](https://github.com/evil8io/drover/commit/7a1b37939671b28af9b08f7aa0637b76b596c4f0))

## [0.14.1](https://github.com/evil8io/drover/compare/v0.14.0...v0.14.1) (2026-09-24)


### Bug Fixes

* **api-filter:** bound the namespaced requests of all fan-outs together ([#73](https://github.com/evil8io/drover/issues/73)) ([24b336a](https://github.com/evil8io/drover/commit/24b336ab195ab256ed8645d2fe803bfed6d4c963))
* **api-filter:** cap the review body at 64 KiB, and grant on the echoed spec only ([#68](https://github.com/evil8io/drover/issues/68)) ([f34a601](https://github.com/evil8io/drover/commit/f34a601fe3b34385e17ac83eff9928b932a6e0e3))
* **api-filter:** key the allowed set on the credential that Rancher reads, and limit the fetches per caller ([#67](https://github.com/evil8io/drover/issues/67)) ([0b53803](https://github.com/evil8io/drover/commit/0b538035c9f202f8d4ee4e8689307d62bea14b7b))
* **api-filter:** reserve a watch slot before the upstream request, per caller and in total ([#69](https://github.com/evil8io/drover/issues/69)) ([7e82e07](https://github.com/evil8io/drover/commit/7e82e0739767ba4d97a879e337836f7bae55d995))
* **api-filter:** select only the projects that contain an allowed namespace ([#70](https://github.com/evil8io/drover/issues/70)) ([bd8040e](https://github.com/evil8io/drover/commit/bd8040e952ea75b232c7b041c744f5a91f55e8e9))
* **project-sync:** page the namespace list, and keep only the keys that the sync reads ([#72](https://github.com/evil8io/drover/issues/72)) ([292e2b3](https://github.com/evil8io/drover/commit/292e2b38926c5fe0f1028554fd16514d4df1f595))

## [0.14.0](https://github.com/evil8io/drover/compare/v0.13.0...v0.14.0) (2026-09-21)


### Features

* **rotate-token:** write the password hash of the service user ([#64](https://github.com/evil8io/drover/issues/64)) ([f629972](https://github.com/evil8io/drover/commit/f6299728b5f9d4dbe3b4586d21277080404e20bd))

## [0.13.0](https://github.com/evil8io/drover/compare/v0.12.1...v0.13.0) (2026-09-21)


### Features

* add a flag that skips the certificate verification of Rancher ([#62](https://github.com/evil8io/drover/issues/62)) ([fb77d3c](https://github.com/evil8io/drover/commit/fb77d3c73ff6c599ac67d6b004b928382eea9ccf))
* **api-filter:** end the initial events of a merged watch-list ([#59](https://github.com/evil8io/drover/issues/59)) ([529bf33](https://github.com/evil8io/drover/commit/529bf3318676f37a28904657b558094a71e8f890))
* **project-sync:** coalesce the watch events, and reconcile the clusters in parallel ([#63](https://github.com/evil8io/drover/issues/63)) ([30d94fa](https://github.com/evil8io/drover/commit/30d94fa08c7b77df3642b20ca09c2d81713c9447))


### Bug Fixes

* **telemetry:** drop the query from url.full on a client span ([#58](https://github.com/evil8io/drover/issues/58)) ([1515891](https://github.com/evil8io/drover/commit/15158911ffbc8395a2c0b8f5c1de07cd29fc8584))

## [0.12.1](https://github.com/evil8io/drover/compare/v0.12.0...v0.12.1) (2026-09-21)


### Bug Fixes

* **api-filter:** close the findings of a security and reliability review ([#57](https://github.com/evil8io/drover/issues/57)) ([06834f9](https://github.com/evil8io/drover/commit/06834f98d2eed274ba0193e9df67acfdd50e2483))
* **project-sync:** remove only a key of the allow list ([#53](https://github.com/evil8io/drover/issues/53)) ([8d9b3fa](https://github.com/evil8io/drover/commit/8d9b3fa0c8ee07c88b564654c3e9261d5da00cd1))
* **rotate-token:** keep the new token whatever its position in the list ([#54](https://github.com/evil8io/drover/issues/54)) ([4bc64e9](https://github.com/evil8io/drover/commit/4bc64e90328d0f63f01ea48c8d96f0a2f61b47bf))

## [0.12.0](https://github.com/evil8io/drover/compare/v0.11.1...v0.12.0) (2026-09-20)


### Features

* merge a cluster-wide watch of a namespaced kind ([#51](https://github.com/evil8io/drover/issues/51)) ([9c80dfa](https://github.com/evil8io/drover/commit/9c80dfa2ed17f4284665a88aea8edbea1b247d7c))

## [0.11.1](https://github.com/evil8io/drover/compare/v0.11.0...v0.11.1) (2026-09-20)


### Bug Fixes

* put the path template on the span as http.route ([#49](https://github.com/evil8io/drover/issues/49)) ([f94c01c](https://github.com/evil8io/drover/commit/f94c01cf375ea4eed424d66a6effb82461917dfb))

## [0.11.0](https://github.com/evil8io/drover/compare/v0.10.2...v0.11.0) (2026-09-20)


### Features

* name a span after the method and a path template ([#47](https://github.com/evil8io/drover/issues/47)) ([bd4d330](https://github.com/evil8io/drover/commit/bd4d3305867cfc16a6dcf090f78ea142b0da62ba))

## [0.10.2](https://github.com/evil8io/drover/compare/v0.10.1...v0.10.2) (2026-09-20)


### Bug Fixes

* keep the kind of a custom resource list in the merged answer ([#45](https://github.com/evil8io/drover/issues/45)) ([1e95e72](https://github.com/evil8io/drover/commit/1e95e72e312cacef0835bf4a9b6e97632d96b69b))

## [0.10.1](https://github.com/evil8io/drover/compare/v0.10.0...v0.10.1) (2026-09-20)


### Bug Fixes

* answer an empty collection when the caller may see no object ([#43](https://github.com/evil8io/drover/issues/43)) ([41f9e1c](https://github.com/evil8io/drover/commit/41f9e1cd3da27f4080dcc034b8ea0758e185896f))

## [0.10.0](https://github.com/evil8io/drover/compare/v0.9.1...v0.10.0) (2026-09-20)


### ⚠ BREAKING CHANGES

* the subcommand `namespace-filter` is now `api-filter`.

### Features

* rename the command to api-filter and merge a cluster-wide list ([#41](https://github.com/evil8io/drover/issues/41)) ([2807c2e](https://github.com/evil8io/drover/commit/2807c2e696c16c28c30b72813d965b5df3194e11))

## [0.9.1](https://github.com/evil8io/drover/compare/v0.9.0...v0.9.1) (2026-09-19)


### Bug Fixes

* end a namespace watch when the selector of the caller changes ([#39](https://github.com/evil8io/drover/issues/39)) ([a8d46c2](https://github.com/evil8io/drover/commit/a8d46c2dd735858c793732849d0a68883c97bb31))

## [0.9.0](https://github.com/evil8io/drover/compare/v0.8.1...v0.9.0) (2026-09-19)


### Features

* watch namespaces, and track the keys that the sync owns ([#37](https://github.com/evil8io/drover/issues/37)) ([35a0999](https://github.com/evil8io/drover/commit/35a09993a8109a6cb525336a5ad23c1652cc2d25))

## [0.8.1](https://github.com/evil8io/drover/compare/v0.8.0...v0.8.1) (2026-09-19)


### Bug Fixes

* read a websocket watch as a byte stream, not one event per message ([#35](https://github.com/evil8io/drover/issues/35)) ([d4b1950](https://github.com/evil8io/drover/commit/d4b1950ae15bb030ae1a898103ba4e732b2170ff))

## [0.8.0](https://github.com/evil8io/drover/compare/v0.7.0...v0.8.0) (2026-09-19)


### Features

* cap the allowed-set cache and its fetch rate ([#32](https://github.com/evil8io/drover/issues/32)) ([c969cf0](https://github.com/evil8io/drover/commit/c969cf02f340d018ef372966500cddcbf7661428))
* copy the project display name to a namespace label or annotation ([#28](https://github.com/evil8io/drover/issues/28)) ([f7502ef](https://github.com/evil8io/drover/commit/f7502ef9b9943fad760048566a9b9d8738a6d6cf))
* give a namespace watch an exact label selector ([#33](https://github.com/evil8io/drover/issues/33)) ([d7b5571](https://github.com/evil8io/drover/commit/d7b557161a229edf5c3fd54a05e8d6d56478c57d))
* name the caller of a namespace-filter request in logs and spans ([#34](https://github.com/evil8io/drover/issues/34)) ([82c79d2](https://github.com/evil8io/drover/commit/82c79d2fc1f61225123a98288577a9d32fe4b4f4))


### Bug Fixes

* filter the namespace watch frames of a websocket upgrade ([#31](https://github.com/evil8io/drover/issues/31)) ([8b4ea2e](https://github.com/evil8io/drover/commit/8b4ea2e61fd34643c83349d501f93dc3eb1bfc09))
* grant a namespace list review only with an allowed namespace ([#29](https://github.com/evil8io/drover/issues/29)) ([684f7d3](https://github.com/evil8io/drover/commit/684f7d3150b8ab564501891cd3015da96fdb59c1))

## [0.7.0](https://github.com/evil8io/drover/compare/v0.6.0...v0.7.0) (2026-09-19)


### Features

* select a namespace list by project, not by name ([#27](https://github.com/evil8io/drover/issues/27)) ([dffbe07](https://github.com/evil8io/drover/commit/dffbe0796220aa29c387effc3009851a61967296))


### Bug Fixes

* stream the Steve allowed-set decode to cut peak memory ([#25](https://github.com/evil8io/drover/issues/25)) ([13f42f4](https://github.com/evil8io/drover/commit/13f42f424186b91506731cc258d6a517071c15cf))

## [0.6.0](https://github.com/evil8io/drover/compare/v0.5.0...v0.6.0) (2026-09-18)


### Features

* add telemetry and instrument the namespace filter ([#22](https://github.com/evil8io/drover/issues/22)) ([09b8e49](https://github.com/evil8io/drover/commit/09b8e494d66091ae7165b1797181732a028e8687))
* add telemetry to project-sync and rotate-token ([#24](https://github.com/evil8io/drover/issues/24)) ([2b6b4cd](https://github.com/evil8io/drover/commit/2b6b4cd7ab188c87d5b0edbbdfa992c03f9112ba))

## [0.5.0](https://github.com/evil8io/drover/compare/v0.4.1...v0.5.0) (2026-09-18)


### Features

* end open watches with EOF before shutdown ([#20](https://github.com/evil8io/drover/issues/20)) ([a7ffbe1](https://github.com/evil8io/drover/commit/a7ffbe1b28ea8ff1d70d9e061ddf1788fa213e58))

## [0.4.1](https://github.com/evil8io/drover/compare/v0.4.0...v0.4.1) (2026-09-18)


### Bug Fixes

* keep a JSON Accept on a namespace watch and read table events ([#18](https://github.com/evil8io/drover/issues/18)) ([46dfb34](https://github.com/evil8io/drover/commit/46dfb34b6f5c8df42450065a2bfac209e29eee68))

## [0.4.0](https://github.com/evil8io/drover/compare/v0.3.2...v0.4.0) (2026-09-18)


### Features

* filter a namespace watch by event, not by a fixed selector ([#16](https://github.com/evil8io/drover/issues/16)) ([bf4ff46](https://github.com/evil8io/drover/commit/bf4ff4683e2d46028525cf0a9f3f166b46f437d6))

## [0.3.2](https://github.com/evil8io/drover/compare/v0.3.1...v0.3.2) (2026-09-18)


### Bug Fixes

* grant a namespace list review that names a namespace ([#14](https://github.com/evil8io/drover/issues/14)) ([defcdd2](https://github.com/evil8io/drover/commit/defcdd24a170f936a407c0a3fe11df584be08066))

## [0.3.1](https://github.com/evil8io/drover/compare/v0.3.0...v0.3.1) (2026-09-18)


### Bug Fixes

* read a protobuf access review ([#12](https://github.com/evil8io/drover/issues/12)) ([04f1289](https://github.com/evil8io/drover/commit/04f12898aeab3f2d29cbacc513920c70e822a78e))

## [0.3.0](https://github.com/evil8io/drover/compare/v0.2.0...v0.3.0) (2026-09-18)


### ⚠ BREAKING CHANGES

* rename to drover and add subcommands ([#7](https://github.com/evil8io/drover/issues/7))

### Features

* add the project-sync subcommand ([#10](https://github.com/evil8io/drover/issues/10)) ([175f824](https://github.com/evil8io/drover/commit/175f82442ba3d235fe0fc3018e3ae5be84bf8e49))
* add the rotate-token subcommand ([#11](https://github.com/evil8io/drover/issues/11)) ([0923031](https://github.com/evil8io/drover/commit/0923031aea832c53f9347cff9e92b8b5962815ca))
* rename to drover and add subcommands ([#7](https://github.com/evil8io/drover/issues/7)) ([3acf1dd](https://github.com/evil8io/drover/commit/3acf1ddfcdfb4f46368f3f607e5941886b7f220d))


### Bug Fixes

* start without a token file ([#9](https://github.com/evil8io/drover/issues/9)) ([1dbf669](https://github.com/evil8io/drover/commit/1dbf669ff0c8226934f5dc7ab38ec9bd0760df10))

## [0.2.0](https://github.com/evil8io/rancher-namespace-filter/compare/v0.1.0...v0.2.0) (2026-09-18)


### Features

* add a Helm chart ([#4](https://github.com/evil8io/rancher-namespace-filter/issues/4)) ([857d73c](https://github.com/evil8io/rancher-namespace-filter/commit/857d73c71f385cfbff2cdbc3b729413c853978ed))

## 0.1.0 (2026-09-18)


### Features

* add the namespace filter service ([#1](https://github.com/evil8io/rancher-namespace-filter/issues/1)) ([03529de](https://github.com/evil8io/rancher-namespace-filter/commit/03529de1dcbbec29889230421c909d6da70c00c4))
