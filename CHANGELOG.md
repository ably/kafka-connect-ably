# Changelog

## [4.1.3](https://github.com/ably/kafka-connect-ably/tree/v4.1.3)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v4.1.2...v4.1.3)

**Fixed bugs:**

- Upgrade msgpack-core to 0.9.11 to address security vulnerability [\#206](https://github.com/ably/kafka-connect-ably/pull/206)

## [4.1.2](https://github.com/ably/kafka-connect-ably/tree/v4.1.2)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v4.1.1...v4.1.2)

- Fixed offset calculation
- Updated dependencies

## [4.1.1](https://github.com/ably/kafka-connect-ably/tree/v4.1.1)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v4.1.0...v4.1.1)

**Implemented enhancements:**

- Update `ably-java` dependency [\#181](https://github.com/ably/kafka-connect-ably/issues/181)
- Encryption support feature request \[parked\] [\#169](https://github.com/ably/kafka-connect-ably/issues/169)

**Fixed bugs:**

- Kafka connector's `client.id` configuration option is ignored during message publishing. [\#186](https://github.com/ably/kafka-connect-ably/issues/186)
- In a batch request each message should be in its own batchspec [\#171](https://github.com/ably/kafka-connect-ably/issues/171)

## [4.1.0](https://github.com/ably/kafka-connect-ably/compare/v4.0.0...v4.1.0)

**Implemented enhancements:**

- Provide option to specify encoding for Ably Message using special Kafka header [\#158](https://github.com/ably/kafka-connect-ably/issues/158)

## [4.0.0](https://github.com/ably/kafka-connect-ably/tree/v4.0.0) (2023-09-21)

This is a major release and contains a number of breaking changes. Please see the [upgrade guide](UPDATING.md) for more information on how to upgrade from version 3.x.

**Implemented enhancements:**

- Stringify Kafka headers before send them to Ably [\#155](https://github.com/ably/kafka-connect-ably/pull/155)
- Flatten Kafka headers in extras [\#155](https://github.com/ably/kafka-connect-ably/pull/155)

## [3.0.0](https://github.com/ably/kafka-connect-ably/tree/v3.0.0)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v2.1.4...v3.0.0)

This is a major release and contains a number of breaking changes. Please see the [upgrade guide](UPDATING.md) for more information on how to upgrade from version 2.x.

**Closed issues:**

- Support multiple `SinkTask` instances [\#138](https://github.com/ably/kafka-connect-ably/issues/138)
- Remove orphaned config [\#135](https://github.com/ably/kafka-connect-ably/issues/135)
- Support `Struct` fields in chanel and message name mapping [\#134](https://github.com/ably/kafka-connect-ably/issues/134)
- Configurable buffering of incoming records [\#133](https://github.com/ably/kafka-connect-ably/issues/133)
- Submit records using Batch API [\#124](https://github.com/ably/kafka-connect-ably/issues/124)
- Test on MSK [\#123](https://github.com/ably/kafka-connect-ably/issues/123)
- Offset tracking and committing [\#121](https://github.com/ably/kafka-connect-ably/issues/121)
- Integration Testing [\#120](https://github.com/ably/kafka-connect-ably/issues/120)
- DLQ Error Handling [\#119](https://github.com/ably/kafka-connect-ably/issues/119)

**Merged pull requests:**

- docs: readme entry for Confluent Cloud BYOC [\#147](https://github.com/ably/kafka-connect-ably/pull/147) ([AndyTWF](https://github.com/AndyTWF))
- v3.0.0 [\#146](https://github.com/ably/kafka-connect-ably/pull/146) ([AndyTWF](https://github.com/AndyTWF))
- Documentation and Config updates [\#142](https://github.com/ably/kafka-connect-ably/pull/142) ([jaley](https://github.com/jaley))
- Structured field access for channel and message name mappings [\#141](https://github.com/ably/kafka-connect-ably/pull/141) ([jaley](https://github.com/jaley))
- Support multiple tasks [\#140](https://github.com/ably/kafka-connect-ably/pull/140) ([jaley](https://github.com/jaley))
- Remove legacy config and documentation [\#139](https://github.com/ably/kafka-connect-ably/pull/139) ([jaley](https://github.com/jaley))
- Restore integration tests and fix related bugs [\#137](https://github.com/ably/kafka-connect-ably/pull/137) ([jaley](https://github.com/jaley))
- Buffer incoming `SinkRecords` before sending batches [\#132](https://github.com/ably/kafka-connect-ably/pull/132) ([jaley](https://github.com/jaley))
- End to end testing [\#131](https://github.com/ably/kafka-connect-ably/pull/131) ([jaley](https://github.com/jaley))
- Added OffsetRegistryService class to store offsets by kafka topic/par… [\#130](https://github.com/ably/kafka-connect-ably/pull/130) ([subkanthi](https://github.com/subkanthi))
- MSK Packaging & Example Deployment [\#129](https://github.com/ably/kafka-connect-ably/pull/129) ([jaley](https://github.com/jaley))
- Kafka DLQ [\#128](https://github.com/ably/kafka-connect-ably/pull/128) ([subkanthi](https://github.com/subkanthi))
- Update to Confluent Platform 7.4.0 [\#127](https://github.com/ably/kafka-connect-ably/pull/127) ([jaley](https://github.com/jaley))
- Fix Docker build errors [\#126](https://github.com/ably/kafka-connect-ably/pull/126) ([jaley](https://github.com/jaley))
- Batch API changes, create Threadpoolexecutor, add logic to batch mess… [\#125](https://github.com/ably/kafka-connect-ably/pull/125) ([subkanthi](https://github.com/subkanthi))

## [v2.1.4](https://github.com/ably/kafka-connect-ably/tree/v2.1.4)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v2.1.3...v2.1.4)

- Bump ably-java to 1.2.27 [\#112](https://github.com/ably/kafka-connect-ably/pull/112) ([AndyTWF](https://github.com/AndyTWF))

## [v2.1.3](https://github.com/ably/kafka-connect-ably/tree/v2.1.3)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v2.1.2...v2.1.3)

**Fixed bugs:**

- Republish messages that were canceled after connection suspension [\#106](https://github.com/ably/kafka-connect-ably/issues/106)

## [v2.1.2](https://github.com/ably/kafka-connect-ably/tree/v2.1.2)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v2.1.1...v2.1.2)

**Implemented enhancements:**

- Use record headers to add push payload [\#101](https://github.com/ably/kafka-connect-ably/issues/101)

**Closed issues:**

- Add support for String keys  [\#98](https://github.com/ably/kafka-connect-ably/issues/98)

**Merged pull requests:**

- Update ably java version [\#103](https://github.com/ably/kafka-connect-ably/pull/103) ([ikbalkaya](https://github.com/ikbalkaya))


## [v2.1.1](https://github.com/ably/kafka-connect-ably/tree/v2.1.1)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v2.1.0...v2.1.1)

**Implemented enhancements:**

- Add logical type support to Json converter [\#99](https://github.com/ably/kafka-connect-ably/pull/99) ([ikbalkaya](https://github.com/ikbalkaya))

## [v2.1.0](https://github.com/ably/kafka-connect-ably/tree/v2.1.0)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v2.0.3...v2.1.0)

**Implemented enhancements:**

- Provide ability for users to channel failed messages into a dead letter queue  [\#90](https://github.com/ably/kafka-connect-ably/issues/90)
- Provide ability to skip a record when a key is absent and channel is configured with a key [\#85](https://github.com/ably/kafka-connect-ably/issues/85)
- Update docker-compose to include latest version of images [\#81](https://github.com/ably/kafka-connect-ably/issues/81)

## [v2.0.3](https://github.com/ably/kafka-connect-ably/tree/v2.0.3)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v2.0.2...v2.0.3)

**Fixed bugs:**

- Support for nullable structs' conversion  [\#86](https://github.com/ably/kafka-connect-ably/issues/86)

**Merged pull requests:**

- Fix NullPointerException issue when null struct value is provided to Json converter [\#87](https://github.com/ably/kafka-connect-ably/pull/87) ([ikbalkaya](https://github.com/ikbalkaya))


## [v2.0.2](https://github.com/ably/kafka-connect-ably/tree/v2.0.2)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v2.0.1...v2.0.2)

This release increases ably-java dependency version to fix a potential vulnerability issue in dependency used by ably-java.

**Merged pull requests:**

- Update ably-java version to 1.2.16 [\#83](https://github.com/ably/kafka-connect-ably/pull/83) ([ikbalkaya](https://github.com/ikbalkaya))

## [v2.0.1](https://github.com/ably/kafka-connect-ably/tree/v2.0.1) (2022-06-01)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v2.0.0...v2.0.1)

This release increases the capability of this connector to process data types received from Kafka Connect,
with support for JSONifying schematic data [\#71](https://github.com/ably/kafka-connect-ably/issues/71), implemented in [\#73](https://github.com/ably/kafka-connect-ably/pull/73) ([ikbalkaya](https://github.com/ikbalkaya)).

## [v2.0.0](https://github.com/ably/kafka-connect-ably/tree/v2.0.0) (2022-04-20)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v1.0.3...v2.0.0)

**Implemented enhancements:**

- Remove client.use.binary.protocol configuration  [\#57](https://github.com/ably/kafka-connect-ably/issues/57)
- Add configuration validators [\#53](https://github.com/ably/kafka-connect-ably/issues/53)
- Make message name configurable and interpolable [\#46](https://github.com/ably/kafka-connect-ably/issues/46)
- Code reorganization and refinement [\#63](https://github.com/ably/kafka-connect-ably/pull/63) ([ikbalkaya](https://github.com/ikbalkaya))
- Add distributed mode configuration file [\#62](https://github.com/ably/kafka-connect-ably/pull/62) ([ikbalkaya](https://github.com/ikbalkaya))
- Remove client.use.binary.protocol configuration [\#60](https://github.com/ably/kafka-connect-ably/pull/60) ([ikbalkaya](https://github.com/ikbalkaya))
- Make message name configurable [\#48](https://github.com/ably/kafka-connect-ably/pull/48) ([ikbalkaya](https://github.com/ikbalkaya))
- Implement pattern based mapping [\#43](https://github.com/ably/kafka-connect-ably/pull/43) ([ikbalkaya](https://github.com/ikbalkaya))

**Fixed bugs:**

- Readme emphasizes n to 1 mapping [\#66](https://github.com/ably/kafka-connect-ably/issues/66)
- NullPointerException when proxy password is not specified [\#34](https://github.com/ably/kafka-connect-ably/issues/34)
- NullPointerException on proxy port issue [\#52](https://github.com/ably/kafka-connect-ably/pull/52) ([ikbalkaya](https://github.com/ikbalkaya))

## [v1.0.3](https://github.com/ably/kafka-connect-ably/tree/v1.0.3) (2021-12-31)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v1.0.2...v1.0.3)

**Fixed bugs:**

- Caught exception and printed stack trace instead of throwing it to resume task [\#36](https://github.com/ably/kafka-connect-ably/pull/36) ([ikbalkaya](https://github.com/ikbalkaya))

## [v1.0.2](https://github.com/ably/kafka-connect-ably/tree/v1.0.2) (2021-09-29)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v1.0.1...v1.0.2)

**Merged pull requests:**

- Document the release process [\#31](https://github.com/ably/kafka-connect-ably/pull/31) ([lmars](https://github.com/lmars))
- Set 'kafka-connect-ably' in the Ably-Agent header [\#30](https://github.com/ably/kafka-connect-ably/pull/30) ([lmars](https://github.com/lmars))
- Conform license and copyright [\#28](https://github.com/ably/kafka-connect-ably/pull/28) ([QuintinWillison](https://github.com/QuintinWillison))
- Conform overview section of readme [\#27](https://github.com/ably/kafka-connect-ably/pull/27) ([QuintinWillison](https://github.com/QuintinWillison))
- Add Maven version for those using ASDF or compatible tooling [\#26](https://github.com/ably/kafka-connect-ably/pull/26) ([QuintinWillison](https://github.com/QuintinWillison))
- Explicitly specify workflow `permissions` required to succeed when our org switches default access from 'permissive' to 'restricted' [\#25](https://github.com/ably/kafka-connect-ably/pull/25) ([QuintinWillison](https://github.com/QuintinWillison))

## [v1.0.1](https://github.com/ably/kafka-connect-ably/tree/v1.0.1) (2021-08-25)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v1.0.0...v1.0.1)

**Closed issues:**

- Issue when installing the connector [\#18](https://github.com/ably/kafka-connect-ably/issues/18)
- Issue when installing the connector [\#17](https://github.com/ably/kafka-connect-ably/issues/17)
- Review Kafka code for Tech Preview [\#16](https://github.com/ably/kafka-connect-ably/issues/16)
- test [\#15](https://github.com/ably/kafka-connect-ably/issues/15)
- Publish to Confluent Hub [\#13](https://github.com/ably/kafka-connect-ably/issues/13)
- Review Kafka code for Tech Preview [\#12](https://github.com/ably/kafka-connect-ably/issues/12)
- Review Kafka code for Tech Preview [\#11](https://github.com/ably/kafka-connect-ably/issues/11)

**Merged pull requests:**

- Add ownerLogo [\#24](https://github.com/ably/kafka-connect-ably/pull/24) ([lmars](https://github.com/lmars))
- Add logo [\#23](https://github.com/ably/kafka-connect-ably/pull/23) ([lmars](https://github.com/lmars))
- Refactor project code [\#22](https://github.com/ably/kafka-connect-ably/pull/22) ([KacperKluka](https://github.com/KacperKluka))
- Use consistent naming: 'Ably Kafka Connector' [\#20](https://github.com/ably/kafka-connect-ably/pull/20) ([lmars](https://github.com/lmars))
- Fix build failure relating to Kafka Connect parent POM [\#19](https://github.com/ably/kafka-connect-ably/pull/19) ([QuintinWillison](https://github.com/QuintinWillison))

## [v1.0.0](https://github.com/ably/kafka-connect-ably/tree/v1.0.0) (2021-06-24)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/238dc7d401067a37b7950bce5d29a9a270a9faaa...v1.0.0)

**Implemented enhancements:**

- Test issue [\#2](https://github.com/ably/kafka-connect-ably/issues/2)

**Closed issues:**

- Update configuration and running locally docs [\#8](https://github.com/ably/kafka-connect-ably/issues/8)
- Migrate kafka-connect-ably Java packages from io.ably to com.ably [\#7](https://github.com/ably/kafka-connect-ably/issues/7)
- Write tests for Kafka Connect Library [\#4](https://github.com/ably/kafka-connect-ably/issues/4)
- Sync test 2 [\#3](https://github.com/ably/kafka-connect-ably/issues/3)

**Merged pull requests:**

- DOC-303: Update README with configuration properties and installation instructions [\#14](https://github.com/ably/kafka-connect-ably/pull/14) ([m-hulbert](https://github.com/m-hulbert))
- Rename package from io.ably.kakfa.\* to com.ably.kafka.\* [\#10](https://github.com/ably/kafka-connect-ably/pull/10) ([lmars](https://github.com/lmars))
- Update configuration and running locally docs [\#6](https://github.com/ably/kafka-connect-ably/pull/6) ([lmars](https://github.com/lmars))
- Add integration test [\#5](https://github.com/ably/kafka-connect-ably/pull/5) ([lmars](https://github.com/lmars))
- Put Kafka key and headers in Ably message extras [\#1](https://github.com/ably/kafka-connect-ably/pull/1) ([lmars](https://github.com/lmars))



\* *This Changelog was automatically generated by [github_changelog_generator](https://github.com/github-changelog-generator/github-changelog-generator)*
