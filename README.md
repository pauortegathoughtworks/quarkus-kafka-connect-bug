# Minimal reproducible project for our Quakus + Kafka Connect upgrade issue

## Prerequisites
- Docker
- direnv

## How to reproduce

1. Clone this repo.
2. Run `docker-compose up`.
3. Run `./gradlew clean build`.
4. Run `./gradlew quarkusDev`.

If you're running on the `3.6.8` version you'll a stack trace with the following error:
```
java.lang.NoClassDefFoundError: javax/ws/rs/core/Configurable
```

If you're running on the `2.16.12.Final` version it will run fine.

## How to switch between versions

1. Go to the `gradle.properties` file and assign either `2.16.12.Final` or `3.6.8` as values for the `quarkusPluginVersion` and `quarkusVersion` properties.
2. Go to the `KafkaConnectRunner.java` file and replace all imports from `javax` to `jakarta` or viceversa.

Don't forget to run `./gradlew clean build` and `./gradlew quarkusDev` again.