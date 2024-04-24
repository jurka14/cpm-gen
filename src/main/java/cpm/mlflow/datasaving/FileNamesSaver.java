package cpm.mlflow.datasaving;

import org.json.JSONArray;
import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;

import java.io.File;

/**
 * Saves the filenames inside a specified directory (or the single filename in case a file is specified).
 * The search is non-recursive.
 * The filenames are saved as a JSON array string.
 */
public class FileNamesSaver extends DataSaver {
    private static final String DATA_TYPE = "xsd:string";
    private final MlflowClient client;

    public FileNamesSaver(MlflowClient client, JSONObject bindings) {
        super(bindings, DATA_TYPE);
        this.client = client;
    }

    @Override
    protected String getData(String runId, JSONObject dataInfo) {

        String path = dataInfo.getString("path");

        File fileOrDIr = client.downloadArtifacts(runId, path);

        return loadFileNames(path, fileOrDIr);
    }

    private String loadFileNames(String path, File file) {
        if (file.isDirectory()) {

            String[] dirContents = file.list();
            String[] empty = {""};
            JSONArray fileNames = new JSONArray();

            for (String name : dirContents != null ? dirContents : empty) {
                fileNames.put(path + name);
            }
            return fileNames.toString();

        } else {
            return path + file.getName();
        }
    }
}
