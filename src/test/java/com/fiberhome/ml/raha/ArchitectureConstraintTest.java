package com.fiberhome.ml.raha;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * 新工程包边界和禁止字段持续集成检查。
 */
class ArchitectureConstraintTest {

    @Test
    void shouldNotContainForbiddenRuntimePackagesOrRepairFields() throws IOException {
        Path root = Paths.get("src/main/java/com/fiberhome/ml/raha");
        List<String> forbiddenDirectories = Arrays.asList("job", "stage", "checkpoint",
                "repository", "parallel", "app", "worker", "queue", "recovery",
                "retry", "correction", "repair");
        for (String directory : forbiddenDirectories) {
            assertFalse(Files.exists(root.resolve(directory)),
                    "发现禁止目录：" + directory);
        }
        try (Stream<Path> files = Files.walk(root)) {
            files.filter(path -> path.toString().endsWith(".java")).forEach(path -> {
                try {
                    String source = new String(Files.readAllBytes(path),
                            StandardCharsets.UTF_8);
                    assertFalse(source.contains("correct_value"));
                    assertFalse(source.contains("repair_value"));
                    assertFalse(source.contains("clean_value"));
                } catch (IOException exception) {
                    throw new IllegalStateException(exception);
                }
            });
        }
        assertTrue(Files.exists(root.resolve("api/RahaFacade.java")));
        assertTrue(Files.exists(root.resolve("udf/F_DW_RAHASAMPLE.java")));
    }
}
