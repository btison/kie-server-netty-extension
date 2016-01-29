package org.kie.server.ext.netty;

import static org.drools.core.util.IoUtils.readBytesFromZipEntry;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import org.drools.compiler.kproject.xml.DependencyFilter;
import org.drools.core.util.IoUtils;
import org.eclipse.aether.artifact.Artifact;
import org.infinispan.protostream.MessageMarshaller;
import org.kie.api.builder.ReleaseId;
import org.kie.scanner.DependencyDescriptor;
import org.kie.server.services.api.KieContainerInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class KieModuleMetaDataImpl {
    
    private static final Logger LOG = LoggerFactory.getLogger(KieModuleMetaDataImpl.class);
    
    private static final List<String> blackListed = initBlackListedPackages();
    
    private final ArtifactResolver artifactResolver;
    
    private KieContainerInstance kieContainerInstance;
    
    private ReleaseId releaseId;
    
    private final DependencyFilter dependencyFilter;
    
    private final Map<URI, File> jars = new HashMap<>();
    
    private final Map<String, String> protoFiles = new HashMap<>();
    
    private final Set<Class<?>> marshallers = new HashSet<>();
    
    public KieModuleMetaDataImpl(KieContainerInstance kieContainerInstance, DependencyFilter dependencyFilter) {
        this.kieContainerInstance = kieContainerInstance;
        this.releaseId = kieContainerInstance.getKieContainer().getReleaseId();
        this.artifactResolver = ArtifactResolver.getResolverFor(releaseId, false);
        this.dependencyFilter = dependencyFilter;
        init();
    }
    
    public Map<String, String> getProtoFiles() {
        return protoFiles;
    }

    public Set<Class<?>> getMarshallers() {
        return marshallers;
    }

    private void init() {
        if (releaseId != null) {
            addArtifact(artifactResolver.resolveArtifact(releaseId));
        }
        for ( DependencyDescriptor dep : artifactResolver.getAllDependecies(dependencyFilter) ) {
            addArtifact( artifactResolver.resolveArtifact( dep.getReleaseId() ) );
        }
    }
    
    private void addArtifact(Artifact artifact) {
        if (artifact != null && artifact.getExtension() != null && artifact.getExtension().equals("jar")) {
            addJar(artifact.getFile());
        }
    }
    
    private void addJar(File jarFile) {
        URI uri = jarFile.toURI();
        if (!jars.containsKey(uri)) {
            jars.put(uri, jarFile);
            scanJar(jarFile);
        }
    }
    
    private void scanJar(File jarFile) {
        ZipFile zipFile = null;
        try {
            zipFile = new ZipFile( jarFile );
            Enumeration< ? extends ZipEntry> entries = zipFile.entries();
            while ( entries.hasMoreElements() ) {
                ZipEntry entry = entries.nextElement();
                String pathName = entry.getName();
                ClassType type = ClassType.fromPath(pathName);
                if (!isBlacklisted(type.packageName)) {
                    //look for proto files
                    if (type.extensionName.equals("proto")) {
                        LOG.info("Proto :" + pathName + " in jar " + jarFile);
                        protoFiles.put(pathName, new String(readBytesFromZipEntry(jarFile, entry), IoUtils.UTF8_CHARSET));    
                    }
                    // look for instances of MessageMarshaller
                    if (type.isClass()) {
                        try {
                            Class<?> clazz = Class.forName(type.getType(), true, kieContainerInstance.getKieContainer().getClassLoader());
                            if (MessageMarshaller.class.isAssignableFrom(clazz)) {
                                LOG.info("Marshaller :" + clazz.getName() + " in jar " + jarFile);
                                marshallers.add(clazz);
                            }
                        } catch (ClassNotFoundException | NoClassDefFoundError e) {
                            LOG.warn("Class not found : " + type.getType());
                        }
                    }
                }
            }
        } catch ( IOException e) {
            throw new RuntimeException( e );
        } finally {
            if ( zipFile != null ) {
                try {
                    zipFile.close();
                } catch ( IOException e ) {
                    throw new RuntimeException( e );
                }
            }
        }
    }
    
    private boolean isBlacklisted(String pkg) {
        for (String s : blackListed) {
            if (pkg.startsWith(s)) {
                return true;
            }
        }
        return false;
    }
    
    private static List<String> initBlackListedPackages() {
        // list of packages to exclude when looking for marshallers and proto files
        List<String> list = new ArrayList<>();
        list.add("protostream");
        list.add("org.jboss.logging");
        list.add("org.infinispan");
        list.add("com.google");
        return list;
    }
    
    private static class ClassType {
        
        private String packageName;
        private String className;
        private String extensionName;
        
        private ClassType() {}
        
        public static ClassType fromPath(String pathName) {
            ClassType type = new ClassType();
            int separator = pathName.lastIndexOf( '/' );
            type.packageName = separator > 0 ? pathName.substring( 0, separator ).replace('/', '.') : "";
            String classNameFull = pathName.substring( separator + 1, pathName.length());
            int extensionSeparator = classNameFull.lastIndexOf(".");
            type.className = extensionSeparator > 0 ? classNameFull.substring( 0, extensionSeparator) 
                                                    :  classNameFull;
            type.extensionName = extensionSeparator > 0 ? classNameFull.substring(extensionSeparator +1) : "";
            return type;
        }
        
        public String getType() {
            return packageName + "." + className;
        }
        
        public boolean isClass() {
            if ("class".equals(extensionName)) {
                return true;
            }
            return false;
        }

    }

}
