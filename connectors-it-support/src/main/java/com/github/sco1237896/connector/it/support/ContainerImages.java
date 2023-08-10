package com.github.sco1237896.connector.it.support;

import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Properties;

import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

public final class ContainerImages {
    private static final Properties PROPERTIES;

    static {
        PROPERTIES = new Properties();

        try (InputStream is = ContainerImages.class.getResourceAsStream("/containers.properties")) {
            PROPERTIES.load(is);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private ContainerImages() {
    }

    public static DockerImageName image(String name) {
        String image = PROPERTIES.getProperty(name);
        if (image == null) {
            throw new RuntimeException("Unknown image " + name);
        }

        return DockerImageName.parse(image);
    }

    public static GenericContainer<?> container(String name) {
        return new GenericContainer<>(image(name));
    }

    public static <T extends GenericContainer<T>> T container(String name, Class<T> type) {
        try {
            Constructor<T> constructor = type.getConstructor(DockerImageName.class);
            return constructor.newInstance(image(name));
        } catch (NoSuchMethodException | InstantiationException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
