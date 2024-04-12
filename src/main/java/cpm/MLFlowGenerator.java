package cpm;

import org.mlflow.tracking.MlflowClient;
import org.openprovenance.prov.model.Document;

public abstract class MLFlowGenerator implements DSProvGenerator {

    private static final String TRACKING_URI = "";
    private MlflowClient client;

    protected MLFlowGenerator() {
        client = new MlflowClient(TRACKING_URI);
    }

    @Override
    public Document generate() {
        client.downloadArtifacts("runid", "artifactpath");
        return null;
    }




}

