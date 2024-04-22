package cpm.mlflow.dataloading;

import org.json.JSONArray;
import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;

import java.io.File;
import java.io.IOException;

public abstract class MLFDataLoader {
    private final MlflowClient client;
    private final JSONObject bindings;
    protected String dataType;

    protected MLFDataLoader(MlflowClient client, JSONObject bindings, String dataType) {
        this.client = client;
        this.bindings = bindings;
        this.dataType = dataType;
    }
    private File loadFile(String runId, String name, String path) {

        File file = client.downloadArtifacts(runId, path);
        saveData(name + "Path", path, "xsd:string");

        return file;
    }

    private void saveData(String name, String data, String type) {
        JSONArray arr = new JSONArray();
        arr.put(new JSONObject().put("@value", data).put("@type", type));
        bindings.put(name, arr);
    }

    public void load(String runId, JSONObject dataInfo) throws IOException {

        String path = dataInfo.getString("path");
        String name = dataInfo.getString("name");

        File file = loadFile(runId, name, path);
        String data = getData(file);
        saveData(name, data, dataType);
    }

    protected abstract String getData(File file) throws IOException;

}


