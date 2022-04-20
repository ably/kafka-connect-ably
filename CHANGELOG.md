# Changelog


## [v2.0.0](https://github.com/ably/kafka-connect-ably/tree/v2.0.0) (2022-04-20)

[Full Changelog](https://github.com/ably/kafka-connect-ably/compare/v2.0.0...HEAD)

**Implemented enhancements:**

- Optimize integration tests [\#58](https://github.com/ably/kafka-connect-ably/issues/58)
- Remove client.use.binary.protocol configuration  [\#57](https://github.com/ably/kafka-connect-ably/issues/57)
- Add configuration validators [\#53](https://github.com/ably/kafka-connect-ably/issues/53)
- Make message name configurable and interpolable [\#46](https://github.com/ably/kafka-connect-ably/issues/46)
- Code reorganization and refinement [\#63](https://github.com/ably/kafka-connect-ably/pull/63) ([ikbalkaya](https://github.com/ikbalkaya))
- Add distributed mode configuration file [\#62](https://github.com/ably/kafka-connect-ably/pull/62) ([ikbalkaya](https://github.com/ikbalkaya))
- Remove client.use.binary.protocol configuration [\#60](https://github.com/ably/kafka-connect-ably/pull/60) ([ikbalkaya](https://github.com/ikbalkaya))
- Add .editorconfig for IntelliJ formatting support [\#54](https://github.com/ably/kafka-connect-ably/pull/54) ([ikbalkaya](https://github.com/ikbalkaya))
- Refactor channel configuration logic [\#50](https://github.com/ably/kafka-connect-ably/pull/50) ([ikbalkaya](https://github.com/ikbalkaya))
- Integration tests for static and pattern based mapping [\#49](https://github.com/ably/kafka-connect-ably/pull/49) ([ikbalkaya](https://github.com/ikbalkaya))
- Make message name configurable [\#48](https://github.com/ably/kafka-connect-ably/pull/48) ([ikbalkaya](https://github.com/ikbalkaya))
- Update language target to 9 to work with IntelliJ [\#47](https://github.com/ably/kafka-connect-ably/pull/47) ([ikbalkaya](https://github.com/ikbalkaya))
- Implement pattern based mapping [\#43](https://github.com/ably/kafka-connect-ably/pull/43) ([ikbalkaya](https://github.com/ikbalkaya))
- Refactor message mapping logic [\#42](https://github.com/ably/kafka-connect-ably/pull/42) ([ikbalkaya](https://github.com/ikbalkaya))

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
