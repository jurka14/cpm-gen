package cpm.pid;

import cpm.pid.uri.PidUriGenerator;

public abstract class PidGenerator {

    protected PidUriGenerator uriGenerator;

    protected PidGenerator(PidUriGenerator uriGenerator) {
        this.uriGenerator = uriGenerator;
    }
    public abstract void generate(String pid, String name);
    public abstract String getNamespace();

    protected String generateUri() {
        return uriGenerator.generate();
    }
}
