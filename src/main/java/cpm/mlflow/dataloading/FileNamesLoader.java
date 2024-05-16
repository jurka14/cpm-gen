package cpm.mlflow.dataloading;

import org.json.JSONArray;
import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;

import java.io.File;
import java.util.Base64;

/**
 * Saves the filenames inside a specified directory (or the single filename in case a file is specified).
 * The search is non-recursive.
 * The filenames are saved as a JSON array string encoded in base64.
 */
public class FileNamesLoader extends DataLoader {
    private static final String DATA_TYPE = "xsd:base64Binary";
    private final MlflowClient client;

    public FileNamesLoader(MlflowClient client) {
        super(DATA_TYPE);
        this.client = client;
    }

    @Override
    protected String getData(String runId, JSONObject dataInfo) {

        String path = dataInfo.getString("path");

        File fileOrDIr = client.downloadArtifacts(runId, path);

        return Base64.getEncoder().encodeToString(loadFileNames(path, fileOrDIr).getBytes());
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
