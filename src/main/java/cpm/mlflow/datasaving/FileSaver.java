package cpm.mlflow.datasaving;

import cpm.mlflow.MLFlowGenConfigException;
import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Base64;

/**
 * Encodes the file data to base64 string
 */
public class FileSaver extends DataSaver {
    private final MlflowClient client;
    private static final String DATA_TYPE = "xsd:base64Binary";

    public FileSaver(MlflowClient client, JSONObject bindings) {
        super(bindings, DATA_TYPE);
        this.client = client;
    }

    private File loadFile(String runId, String name, String path) {

        File file = client.downloadArtifacts(runId, path);
        saveBinding(name + "Path", path, "xsd:string");

        return file;
    }

    @Override
    protected String getData(String runId, JSONObject dataInfo) {

        String path = dataInfo.getString("path");
        String name = dataInfo.getString("name");

        File file = loadFile(runId, name, path);

        try {
            return Base64.getEncoder().encodeToString(getFileData(file));
        } catch (IOException e) {
            throw new MLFlowGenConfigException(e);
        }
    }

    protected byte[] getFileData(File file) throws IOException {
        return Files.readAllBytes(file.toPath());
    }

}