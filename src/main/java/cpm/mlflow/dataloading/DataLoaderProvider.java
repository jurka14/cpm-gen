package cpm.mlflow.dataloading;

import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;

import java.util.Map;

public class DataLoaderProvider {
    private final Map<String, DataLoader> saverMap;
    public DataLoaderProvider(String clientTrackingUri, JSONObject bindings) {
        MlflowClient client = new MlflowClient(clientTrackingUri);

        DataLoader dataLoader = new DataLoader(bindings, "xsd:string");
        FileLoader fileLoader = new FileLoader(client, bindings);
        MetricLoader metricLoader = new MetricLoader(client, bindings);
        FileNamesLoader fileNamesLoader = new FileNamesLoader(client, bindings);

        saverMap = Map.of(
                "data", dataLoader,
                "file", fileLoader,
                "config", fileLoader,
                "inParquet", fileLoader,
                "outParquet", fileLoader,
                "metrics", metricLoader,
                "filenames", fileNamesLoader
        );
    }

    public DataLoader getLoader(String key) {
        return saverMap.get(key);
    }


}
