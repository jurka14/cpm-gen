package cpm.mlflow.dataloading;

import org.mlflow.tracking.MlflowClient;

import java.util.Map;

public class DataLoaderProvider {
    private final Map<String, DataLoader> saverMap;
    public DataLoaderProvider(String clientTrackingUri) {
        MlflowClient client = new MlflowClient(clientTrackingUri);

        DataLoader dataLoader = new DataLoader("xsd:string");
        FileLoader fileLoader = new FileLoader(client);
        MetricLoader metricLoader = new MetricLoader(client);
        FileNamesLoader fileNamesLoader = new FileNamesLoader(client);
        RunInfoLoader runInfoLoader = new RunInfoLoader(client);

        saverMap = Map.of(
                "data", dataLoader,
                "file", fileLoader,
                "config", fileLoader,
                "inParquet", fileLoader,
                "outParquet", fileLoader,
                "metrics", metricLoader,
                "filenames", fileNamesLoader,
                "runInfo", runInfoLoader
        );
    }

    public DataLoader getLoader(String key) {
        return saverMap.get(key);
    }


}
