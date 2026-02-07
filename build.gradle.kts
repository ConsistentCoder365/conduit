plugins {
    java
    application
}

group = "io.conduit"
version = "0.1.0"

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

repositories {
    mavenCentral()
}

val bench: SourceSet by sourceSets.creating {
    compileClasspath += sourceSets.main.get().output
    runtimeClasspath += sourceSets.main.get().output
}
configurations[bench.implementationConfigurationName].extendsFrom(configurations.implementation.get())
configurations[bench.runtimeOnlyConfigurationName].extendsFrom(configurations.runtimeOnly.get())

dependencies {
    implementation("com.fasterxml.jackson.core:jackson-databind:2.18.2")
    implementation("com.fasterxml.jackson.dataformat:jackson-dataformat-yaml:2.18.2")
    implementation("io.micrometer:micrometer-registry-prometheus:1.14.2")
    implementation("org.postgresql:postgresql:42.7.4")
    implementation("org.neo4j.driver:neo4j-java-driver:5.27.0")
    implementation("org.slf4j:slf4j-api:2.0.16")
    runtimeOnly("ch.qos.logback:logback-classic:1.5.12")

    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
    testImplementation("org.assertj:assertj-core:3.27.0")
    testImplementation(platform("org.testcontainers:testcontainers-bom:1.21.4"))
    testImplementation("org.testcontainers:junit-jupiter")
    testImplementation("org.testcontainers:postgresql")
    testImplementation("org.testcontainers:neo4j")
}

application {
    mainClass = "io.conduit.Main"
    applicationDefaultJvmArgs = listOf("-XX:+UseG1GC", "-XX:MaxGCPauseMillis=20", "-Xms512m", "-Xmx1g")
}

tasks.withType<JavaCompile>().configureEach {
    options.encoding = "UTF-8"
    options.compilerArgs.addAll(listOf("-Xlint:all,-serial,-processing,-try,-this-escape", "-Werror"))
}

tasks.test {
    useJUnitPlatform {
        if (!project.hasProperty("withStress")) excludeTags("stress-long")
    }
    maxHeapSize = "1g"
    testLogging {
        events("failed", "skipped")
        exceptionFormat = org.gradle.api.tasks.testing.logging.TestExceptionFormat.FULL
        showStandardStreams = false
    }
}

fun benchTask(name: String, defaultArgs: String, desc: String) = tasks.register<JavaExec>(name) {
    group = "benchmark"
    description = desc
    classpath = bench.runtimeClasspath
    mainClass = "io.conduit.bench.Benchmark"
    jvmArgs(listOf("-XX:+UseG1GC", "-XX:MaxGCPauseMillis=20", "-Xms1g", "-Xmx1g") +
            (project.findProperty("benchJvmArgs") as String? ?: "").split(" ").filter { it.isNotBlank() })
    args((project.findProperty("benchArgs") as String? ?: defaultArgs).split(" ").filter { it.isNotBlank() })
    workingDir = projectDir
}

benchTask("bench", "all", "Runs the full benchmark suite and writes results to bench/results")
benchTask("benchSmoke", "smoke", "Short throughput run that fails if it drops sharply below bench/baseline.json")
