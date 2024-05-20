package cpm.mlflow.dataloading;

import cpm.mlflow.MLFlowGenConfigException;
import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;

import java.io.File;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.file.Files;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

/**
 * Encodes the file data to base64 string, also loads the file path string representation and its SHA-256 checksum.
 */
public class FileLoader extends DataLoader {
    private final MlflowClient client;
    private static final String DATA_TYPE = "xsd:base64Binary";

    public FileLoader(MlflowClient client) {
        super(DATA_TYPE);
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
        byte[] fileData;

        try {
            fileData = getFileData(file);

            byte[] hash = MessageDigest.getInstance("SHA-256").digest(fileData);
            String checksum = new BigInteger(1, hash).toString(16);
            saveBinding(name + "Hash", checksum, "xsd:hexBinary");

            return Base64.getEncoder().encodeToString(fileData);
        } catch (IOException e) {
            throw new MLFlowGenConfigException(e);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    protected byte[] getFileData(File file) throws IOException {
        return Files.readAllBytes(file.toPath());
    }

}