# redshift-fake-driver

[![Build Status](https://travis-ci.org/opt-tech/redshift-fake-driver.svg?branch=master)](https://travis-ci.org/opt-tech/redshift-fake-driver)

redshift-fake-driver is a JDBC driver accepting Redshift specific commands (e.g. COPY, UNLOAD), which is useful for local testing.

The driver uses `AWS SDK for Java` to connect to S3, so you can use mocking libraries to emulate S3. (e.g. [fake-s3](https://github.com/jubos/fake-s3))

## Supported Redshift Commands
- COPY
  - JSON with jsonpaths
  - MANIFEST
- UNLOAD
- DDLs
  - just drop Redshift specific directives (e.g. DISTSTYLE, DISTKEY, ...)

Some options are currently not supported.

Contributions are welcome !

## Requirements
- S3 ([fake-s3](https://github.com/jubos/fake-s3) is OK)
- `aws-java-sdk-s3` >= 1.10.8
- `postgresql` or `h2` JDBC driver
- Java >= 1.7
- (If you use in Scala projects) Scala 2.10.x / 2.11.x / 2.12.x(Java 1.8)

## Installation
### Maven Java projects

- Add following dependency to `pom.xml`

```
<dependency>
    <groupId>jp.ne.opt</groupId>
    <artifactId>redshift-fake-driver_2.11</artifactId>
    <version>1.0.2</version>
</dependency>
```

- Add other required dependencies (aws-java-sdk-s3, h2 or postgresql driver) to `pom.xml`.

### SBT Projects
- Add following dependency to `build.sbt`
```
"jp.ne.opt" %% "redshift-fake-driver" % "1.0.2"
```

- Add other required dependencies (aws-java-sdk-s3, h2 or postgresql driver) to `build.sbt`.

## Usage (with Postgresql)

### Setup Postgresql
- Start postgresql database to mock Redshift.

### Write an application

#### Example in Scala

```scala
Class.forName("jp.ne.opt.redshiftfake.postgres.FakePostgresqlDriver")

val endpoint = "http://localhost:9444/" // in production, scheme  will be "s3://"

val url = "jdbc:postgresqlredshift://localhost:5432/foo"
val prop = new Properties()
prop.setProperty("driver", "jp.ne.opt.redshiftfake.postgres.FakePostgresqlDriver")
prop.setProperty("user", "sa")

val conn = DriverManager.getConnection(url, prop)
val stmt = conn.createStatement()

val unloadSql =
  s"""unload ('select * from foo') to '${endpoint}foo/unloaded_'
      |credentials 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY' 
      |manifest""".stripMargin
stmt.execute(unloadSql)

val copySql =
  s"""copy bar from '${endpoint}foo/unloaded_manifest'
      |credentials 'aws_access_key_id=AKIAXXXXXXXXXXXXXXX;aws_secret_access_key=YYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYYY'
      |manifest""".stripMargin

stmt.execute(copySql)
```

### Run the application.
#### With real S3
- Specify region via `fake.awsRegion` system property.
  - `ap-northeast-1` by default

#### With fake-s3
- Specify s3 endpoint via `fake.awsS3Endpoint` system property.
  - if you started a `fake-s3` server on `http://localhost:9444/`, specify `-Dfake.awsS3Endpoint="http://localhost:9444/"`. (trailing slash is needed)

## License

Apache 2.0
