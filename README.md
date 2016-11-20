# redshift-fake-driver

[![Build Status](https://travis-ci.org/opt-tech/redshift-fake-driver.svg?branch=master)](https://travis-ci.org/opt-tech/redshift-fake-driver)

redshift-fake-driver is a JDBC driver accepting Redshift specific commands (e.g. COPY, UNLOAD), which is useful for local testing.
The driver uses `AWS SDK for Java` to connect to S3, so you can use mocking libraries to emulate S3. (e.g. [fake-s3](https://github.com/jubos/fake-s3))

## Supported Redshift Commands
- COPY
  - JSON with jsonpaths
  - CSV (T.B.D.)
  - MANIFEST
- UNLOAD
- DDLs
  - just drop Redshift specific directives (e.g. DISTSTYLE, DISTKEY, ...)

Some options are currently not working.
Contributions are welcome !

## Requirements
- S3 ([fake-s3](https://github.com/jubos/fake-s3) is OK)
- `aws-java-sdk-s3` >= 1.10.8
- `postgresql` or `h2` JDBC driver
- Java >= 1.7
- (If you use in Scala projects) Scala 2.10.x or 2.11.x

## Installation
### Maven Java projects

- Add following dependency to `pom.xml`

```
<dependency>
    <groupId>jp.ne.opt</groupId>
    <artifactId>redshift-fake-driver_2.11</artifactId>
    <version>1.0.0</version>
</dependency>
```

- Add other required dependencies (aws-java-sdk-s3, h2 or postgresql driver) to `pom.xml`.

### SBT Projects
- Add following dependency to `build.sbt`
```
"jp.ne.opt" %% "redshift-fake-driver" % "1.0.0"
```

- Add other required dependencies (aws-java-sdk-s3, h2 or postgresql driver) to `build.sbt`.

## Usage (with Postgresql)

### Setup Postgresql
- Start postgresql database to mock Redshift.

### Write an application.
You will use configuration files to configure connection settings.
You should configure Redshift connection as follows only in local environment.

- driver
  - `jp.ne.opt.redshiftfake.postgres.FakePostgresqlDriver`
- url
  - replace `jdbc:postgresql` with `jdbc:postgresqlredshift`

### Run the application.
#### With real S3
- Specify region via `fake.awsRegion` system property.
  - If you do not specify region, the default is `ap-northeast-1`.

### With fake-s3
- Specify s3 endpoint via `fake.awsS3Endpoint` system property.
  - if you started a `fake-s3` server on `http://localhost:9444/`, system property will be `-Dfake.awsS3Endpoint="http://localhost:9444/"`. (trailing slash is needed)

## License
