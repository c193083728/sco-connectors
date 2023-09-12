package com.github.sco1237896.tools.maven.connector;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import com.github.sco1237896.tools.maven.connector.support.Builders;
import com.github.sco1237896.tools.maven.connector.support.ConnectorDefinition;
import com.github.sco1237896.tools.maven.connector.support.ConnectorDependency;
import com.github.sco1237896.tools.maven.connector.support.ConnectorSupport;
import com.github.sco1237896.tools.maven.connector.support.KameletsCatalog;
import com.github.sco1237896.tools.maven.connector.support.MojoSupport;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.maven.execution.MavenSession;
import org.apache.maven.plugin.AbstractMojo;
import org.apache.maven.plugin.MojoExecutionException;
import org.apache.maven.plugin.MojoFailureException;
import org.apache.maven.plugins.annotations.LifecyclePhase;
import org.apache.maven.plugins.annotations.Mojo;
import org.apache.maven.plugins.annotations.Parameter;
import org.apache.maven.plugins.annotations.ResolutionScope;
import org.apache.maven.project.MavenProject;
import org.l2x6.pom.tuner.Comparators;
import org.l2x6.pom.tuner.model.Gavtcs;

@Mojo(name = "enrich-pom", defaultPhase = LifecyclePhase.VALIDATE, requiresDependencyResolution = ResolutionScope.COMPILE_PLUS_RUNTIME, requiresDependencyCollection = ResolutionScope.COMPILE_PLUS_RUNTIME)
public class EnrichPomMojo extends AbstractMojo {

    @Parameter(defaultValue = "false", property = "connector.pom.enrich.skip")
    private boolean skip = false;
    @Parameter(defaultValue = "true", property = "connector.pom.enrich.fail")
    private boolean fail = true;
    @Parameter(defaultValue = "${camel-quarkus.version}")
    private String camelQuarkusVersion;

    @Parameter(readonly = true, defaultValue = "${project}")
    private MavenProject project;

    @Parameter(defaultValue = "${session}", readonly = true)
    protected MavenSession session;
    @Parameter
    private ConnectorDefinition defaults;
    @Parameter
    private List<ConnectorDefinition> connectors;
    @Parameter
    private List<String> bannedDependencies;

    @Override
    public void execute() throws MojoExecutionException, MojoFailureException {
        if (skip) {
            getLog().info("Skipping POM Enricher");
            return;
        }

        String oldDigest = pomChecksum();
        enrichPom();
        String newDigest = pomChecksum();

        if (fail && !Objects.equals(oldDigest, newDigest)) {
            throw new MojoExecutionException(
                    "The dependencies have changed and the pom.xml has been overwritten, please rebuild");
        }
    }

    private void enrichPom() throws MojoExecutionException {
        var builder = new Builders.Pom();
        builder = builder.withPath(project.getFile().toPath());

        try {
            builder = builder.withTransformation((document, context) -> {
                var profile = context.getOrAddProfile("kamelets-deps");
                profile.prependCommentIfNeeded("This is auto generate, do not change it");
                profile.getOrAddChildContainerElement("activation").addChildTextElementIfNeeded(
                        "activeByDefault",
                        "true",
                        Comparators.entryValueOnly());

                var deps = profile.getOrAddChildContainerElement("dependencies");

                for (var child : deps.childElements()) {
                    child.remove(true, true);
                }

                try {
                    for (var dep : injectedDependencies()) {
                        deps.addGavtcsIfNeeded(asGavtcs(dep), Gavtcs.groupFirstComparator());
                    }
                } catch (MojoExecutionException e) {
                    throw new RuntimeException(e);
                }
            });

            getLog().info("Writing pom.xml");

            builder.build();
        } catch (Exception e) {
            throw new MojoExecutionException(e);
        }

    }

    private String pomChecksum() throws MojoExecutionException {
        try (InputStream is = Files.newInputStream(project.getFile().toPath())) {
            return DigestUtils.sha256Hex(is);
        } catch (IOException e) {
            throw new MojoExecutionException(e);
        }
    }

    private Gavtcs asGavtcs(ConnectorDependency cd) {
        return new Gavtcs(cd.groupId, cd.artifactId, null);
    }

    private Set<ConnectorDependency> injectedDependencies() throws MojoExecutionException {
        Set<ConnectorDependency> result = new TreeSet<>(Comparator.comparing(ConnectorDependency::toString));

        try {
            for (ConnectorDefinition connector : MojoSupport.inject(session, defaults, connectors)) {
                Collection<ConnectorDependency> deps = ConnectorSupport.dependencies(
                        getLog(),
                        KameletsCatalog.get(project, getLog()),
                        connector,
                        camelQuarkusVersion);

                deps.stream()
                        .filter(cd -> !isBanned(cd))
                        .map(cd -> new ConnectorDependency(cd.groupId, cd.artifactId))
                        .forEach(result::add);
            }
        } catch (Exception e) {
            throw new MojoExecutionException(e);
        }

        return result;
    }

    private boolean isBanned(ConnectorDependency cd) {
        return bannedDependencies != null && bannedDependencies.contains(cd.groupId + ":" + cd.artifactId);
    }
}
