plugins {
    `java-library`
    id("net.neoforged.moddev") version "2.0.107"
}

base {
    archivesName = "Transerver"
}

dependencies {
    implementation(project(":core"))
}

neoForge {
    version = providers.gradleProperty("neo_version").get()

    runs {
        create("server") {
            server()
        }
    }

    mods {
        create("transerver") {
            sourceSet(sourceSets.main.get())
        }
    }
}

tasks.processResources {
    val properties = mapOf(
        "mod_version" to project.version,
        "minecraft_version_range" to providers.gradleProperty("minecraft_version_range").get(),
        "loader_version_range" to providers.gradleProperty("loader_version_range").get()
    )
    inputs.properties(properties)
    filesMatching("META-INF/neoforge.mods.toml") {
        expand(properties)
    }
}

val coreSourceSets = project(":core").extensions.getByType<JavaPluginExtension>().sourceSets

tasks.jar {
    duplicatesStrategy = DuplicatesStrategy.FAIL
    from(coreSourceSets.getByName("main").output)
    manifest {
        attributes["Automatic-Module-Name"] = "dev.transerver"
        attributes["Main-Class"] = "dev.transerver.core.TranserverRouterMain"
    }
}

tasks.named<Jar>("sourcesJar") {
    from(coreSourceSets.getByName("main").allSource)
}
