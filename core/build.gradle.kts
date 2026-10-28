plugins {
    `java-library`
    application
}

base {
    archivesName = "Transerver"
}

application {
    applicationName = "Transerver"
    mainClass = "dev.transerver.core.TranserverRouterMain"
}

dependencies {
    testImplementation(platform("org.junit:junit-bom:5.11.4"))
    testImplementation("org.junit.jupiter:junit-jupiter")
    testRuntimeOnly("org.junit.platform:junit-platform-launcher")
}

tasks.jar {
    manifest {
        attributes["Automatic-Module-Name"] = "dev.transerver"
        attributes["Main-Class"] = "dev.transerver.core.TranserverRouterMain"
    }
}
