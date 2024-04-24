package cpm.mlflow.datasaving;

import org.json.JSONObject;
import org.mlflow.tracking.MlflowClient;

import java.util.Map;

public class DataSaverProvider {
    private final Map<String, DataSaver> saverMap;
    public DataSaverProvider(String clientTrackingUri, JSONObject bindings) {
        MlflowClient client = new MlflowClient(clientTrackingUri);

        DataSaver dataSaver = new DataSaver(bindings, "xsd:string");
        FileSaver fileSaver = new FileSaver(client, bindings, "xsd:string");
        ConfigSaver configSaver = new ConfigSaver(client, bindings);
        ParquetSaver parquetSaver = new ParquetSaver(client, bindings);
        MetricSaver metricSaver = new MetricSaver(client, bindings);
        FileNamesSaver fileNamesSaver = new FileNamesSaver(client, bindings);

        saverMap = Map.of(
                "data", dataSaver,
                "file", fileSaver,
                "config", configSaver,
                "inParquet", parquetSaver,
                "outParquet", parquetSaver,
                "metrics", metricSaver,
                "filenames", fileNamesSaver
        );
    }

    public DataSaver getSaver(String key) {
        return saverMap.get(key);
    }


}
