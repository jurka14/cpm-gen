package cpm;

import org.json.JSONArray;
import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;

import java.io.File;
import java.io.IOException;

public abstract class MLFDataLoader {

    private static final String TRACKING_URI = "https://mlflow.rationai.cloud.trusted.e-infra.cz/";
    private static final MlflowClient client = new MlflowClient(TRACKING_URI);

    private final JSONObject bindings;
    protected String dataType;

    protected MLFDataLoader(JSONObject bindings, String dataType) {
        this.bindings = bindings;
        this.dataType = dataType;
    }
    private File loadFile(String runId, String name, String path) {

        File file = client.downloadArtifacts(runId, path);
        bindings.put(name + "Path", path);

        return file;
    }

    private void loadData(String name, String data, String type) {
        JSONArray arr = new JSONArray();
        arr.put(new JSONObject().put("@value", data).put("@type", type));
        bindings.put(name, arr);
    }

    public void load(String runId, JSONObject dataInfo) throws IOException {

        String path = dataInfo.getString("path");
        String name = dataInfo.getString("name");

        File file = loadFile(runId, name, path);
        String data = getData(file);
        loadData(name, data, dataType);
    }

    protected abstract String getData(File file) throws IOException;

}


