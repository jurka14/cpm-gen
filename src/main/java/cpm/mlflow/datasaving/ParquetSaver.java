package cpm.mlflow.datasaving;

import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;

/**
 * Encodes parquet data to base64 string
 */
public class ParquetSaver extends FileSaver {

    private static final String DATA_TYPE = "xsd:base64Binary";

    public ParquetSaver(MlflowClient client, JSONObject bindings) {
        super(client, bindings, DATA_TYPE);
    }

    @Override
    protected String getFileData(File file) throws IOException {
        return Base64.getEncoder().encodeToString(Files.readAllBytes(file.toPath()));
    }
}
