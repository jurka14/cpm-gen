package cpm;

public class MLFlowGenException extends RuntimeException{
    public MLFlowGenException(Exception e) {
        super(e.getMessage());
    }
}
