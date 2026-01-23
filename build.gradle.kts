plugins {
    java
    `maven-publish`
}

group = "com.ably.kafka.connect"
version = "4.1.3-SNAPSHOT"

java {
    sourceCompatibility = JavaVersion.VERSION_11
    targetCompatibility = JavaVersion.VERSION_11
    withJavadocJar()
    withSourcesJar()
}

repositories {
    mavenCentral()
    maven {
        url = uri("https://packages.confluent.io/maven/")
    }
    maven {
        url = uri("https://jitpack.io")
    }
}

dependencies {
    // Kafka Connect API - provided by runtime
    compileOnly(libs.kafka.connect.api)
    compileOnly(libs.kafka.connect.transforms)

    // Kafka Connect utilities
    // (not supported since 2023, compiled against kafka 2.8, it's better to get rid of it)
    implementation(libs.connect.utils)

    // Runtime dependencies
    implementation(libs.ably.java)
    implementation(libs.msgpack.core)
    implementation(libs.gson)
    implementation(libs.guava)
    implementation(libs.slf4j.api)

    // Test dependencies
    testImplementation(libs.connect.utils.testing)
    testImplementation(libs.jose4j)

    // Kafka test dependencies (using catalog reference for standard deps)
    testImplementation(libs.kafka.kafka)
    testImplementation(libs.kafka.connect.runtime)
    testImplementation(variantOf(libs.kafka.kafka) { classifier("test") })
    testImplementation(variantOf(libs.kafka.clients.test) { classifier("test") })
    testImplementation(variantOf(libs.kafka.connect.runtime) { classifier("test") })
    testImplementation(variantOf(libs.kafka.server.common.test) { classifier("test") })

    // Confluent test dependencies
    testImplementation(libs.confluent.schema.registry)
    testImplementation(libs.confluent.avro.converter)
    testImplementation(libs.confluent.avro.data)

    // Other test dependencies
    testImplementation(libs.json2avro)
    testImplementation(libs.slf4j.simple)
    testImplementation(libs.junit.jupiter.api)
    testRuntimeOnly(libs.junit.jupiter.engine)
}

tasks.test {
    useJUnitPlatform()
    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

val unitTest by tasks.registering(Test::class) {
    description = "Run unit tests (excludes integration tests)"
    group = "verification"

    useJUnitPlatform {
        excludeTags("integration")
    }

    exclude("**/integration/**")

    testLogging {
        events("passed", "skipped", "failed")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
    }
}

tasks.jar {
    manifest {
        attributes(
            "Implementation-Title" to project.name,
            "Implementation-Version" to project.version,
            "Implementation-Vendor" to "Ably"
        )
    }
}

// Task to create Confluent Hub archive
val confluentArchive by tasks.registering(Zip::class) {
    dependsOn(tasks.jar)

    archiveBaseName.set("kafka-connect-ably")
    archiveVersion.set(project.version.toString())
    destinationDirectory.set(layout.buildDirectory.dir("distributions/confluent"))

    // Include the main connector JAR (slim, without dependencies)
    from(tasks.jar.get().outputs.files) {
        into("lib")
    }

    // Copy all runtime dependencies as separate JARs
    from(configurations.runtimeClasspath) {
        into("lib")
    }

    from("logos") {
        into("assets")
    }

    // Create manifest.json for Confluent Hub
    val manifestFile = file("${layout.buildDirectory.get().asFile}/tmp/manifest.json")
    doFirst {
        manifestFile.parentFile.mkdirs()
        manifestFile.writeText("""
            {
              "name": "kafka-connect-ably",
              "version": "${project.version}",
              "title": "Ably Kafka Connector",
              "description": "The Ably Kafka Connector is a sink connector used to publish data from Apache Kafka into Ably.",
              "owner": {
                "username": "ably",
                "type": "organization",
                "name": "Ably",
                "url": "https://ably.com/",
                "logo": "assets/ably.png"
              },
              "support": {
                "provider_name": "Ably",
                "url": "https://ably.com/support",
                "logo": "assets/ably.png",
                "summary": "This connector is supported by Ably"
              },
              "tags": ["Ably", "realtime", "kafka-connect-ably"],
              "features": {
                "supported_encodings": ["any"],
                "single_message_transforms": true,
                "confluent_control_center_integration": true,
                "kafka_connect_api": true
              },
              "documentation_url": "https://github.com/ably/kafka-connect-ably",
              "component_types": ["sink"]
            }
        """.trimIndent())
    }

    from(manifestFile) {
        into(".")
    }

    from("config") {
        into("config")
    }

    from("README.md") {
      into("doc")
    }
    from("LICENSE") {
      into("doc")
    }
}

// Task to create MSK Connect archive (flat structure with all dependencies)
val mskArchive by tasks.registering(Zip::class) {
    dependsOn(tasks.jar)

    archiveBaseName.set("kafka-connect-ably")
    archiveVersion.set(project.version.toString())
    archiveClassifier.set("bin")
    destinationDirectory.set(layout.buildDirectory.dir("distributions/msk"))

    // Include the main connector JAR
    from(tasks.jar.get().outputs.files)

    // Include all runtime dependencies as separate JARs
    from(configurations.runtimeClasspath)
}

tasks.assemble {
    dependsOn(confluentArchive, mskArchive)
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])

            pom {
                name.set("kafka-connect-ably")
                description.set("A sink connector for publishing data from Apache Kafka into Ably")
                url.set("https://github.com/ably/kafka-connect-ably")

                licenses {
                    license {
                        name.set("The Apache License, Version 2.0")
                        url.set("https://www.apache.org/licenses/LICENSE-2.0")
                    }
                }

                developers {
                    developer {
                        id.set("lmars")
                        name.set("Lewis Marshall")
                        email.set("lewis.marshall@ably.com")
                        url.set("https://github.com/lmars")
                        timezone.set("Europe/London")
                        roles.set(listOf("maintainer"))
                    }
                }

                scm {
                    connection.set("scm:git:https://github.com/ably/kafka-connect-ably.git")
                    developerConnection.set("scm:git:git@github.com:ably/kafka-connect-ably.git")
                    url.set("https://github.com/ably/kafka-connect-ably")
                }

                issueManagement {
                    system.set("github")
                    url.set("https://github.com/ably/kafka-connect-ably/issues")
                }
            }
        }
    }
}

tasks.javadoc {
    options {
        this as StandardJavadocDocletOptions
        addStringOption("Xdoclint:all,-missing", "-quiet")
    }
}
