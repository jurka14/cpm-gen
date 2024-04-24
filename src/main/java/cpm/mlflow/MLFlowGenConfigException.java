package cpm.mlflow;

public class MLFlowGenConfigException extends RuntimeException {
    public MLFlowGenConfigException(Exception e) {
        super(e.getMessage());
    }
}
