package cpm.mlflow.datasaving;

import cpm.mlflow.MLFlowGenConfigException;
import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;

public class FileSaver extends DataSaver {
    private final MlflowClient client;

    public FileSaver(MlflowClient client, JSONObject bindings, String dataType) {
        super(bindings, dataType);
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
            return getFileData(file);
        } catch (IOException e) {
            throw new MLFlowGenConfigException(e);
        }
    }

    protected String getFileData(File file) throws IOException {
        return Files.readString(file.toPath());
    }

}