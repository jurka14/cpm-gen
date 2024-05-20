package cpm;

import cpm.mlflow.MLFlowGenerator;
import cpm.mlflow.dataloading.DataLoaderProvider;
import cpm.pid.DummyPidGenerator;
import cpm.pid.PidGenerator;
import cpm.pid.uri.DummyPidUriGenerator;
import org.openprovenance.prov.interop.Formats;
import org.openprovenance.prov.interop.InteropFramework;
import org.openprovenance.prov.model.Document;

import java.io.IOException;
import java.util.Map;

public class Main {

    private static final InteropFramework intF = new InteropFramework();
    private static final String TRACKING_URI = "https://mlflow.rationai.cloud.trusted.e-infra.cz/";

    private static void generateProvenance(Formats.ProvFormat format) {

        CpmGenerator gen = new CpmGenerator();
        MLFlowGenerator mlfGen = new MLFlowGenerator(new DataLoaderProvider(TRACKING_URI));
        PidGenerator pidGen = new DummyPidGenerator(new DummyPidUriGenerator());

        generateBundle(gen, mlfGen, pidGen, "preprocEval", true, format);
        generateBundle(gen, mlfGen, pidGen, "preprocTrain", true, format);
        generateBundle(gen, mlfGen, pidGen, "train", false, format);
        generateBundle(gen, mlfGen, pidGen, "eval", false, format);
    }

    private static void generateBundle(CpmGenerator gen, MLFlowGenerator mlfGen, PidGenerator pidGen, String name, boolean first, Formats.ProvFormat format) {

        String namewfolder = name + "/" + name;

        Document dsDoc = mlfGen.generate(namewfolder + "_config.json");
        Document doc;

        try {
            doc = gen.createBundle(namewfolder + "_bindings_bb.json", dsDoc, pidGen, first);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        intF.writeDocument(name + "." + intF.getExtension(format), doc);
        intF.writeDocument(name + ".svg", doc);
    }

    public static void main(String [] args) {

        //generateProvenance(Formats.ProvFormat.PROVN);
        query();
    }

    private static void query() {
        Document preprocEval = intF.readDocumentFromFile("preprocEval.provn");

        Document preprocTrain = intF.readDocumentFromFile("preprocTrain.provn");

        Document train = intF.readDocumentFromFile("train.provn");

        Document eval = intF.readDocumentFromFile("eval.provn");

        Map<String, Document> documentMap = Map.of(
                "http://10.16.48.121:42069/api/v1/organizations/ORG/graphs/preprocEval", preprocEval,
                "http://10.16.48.121:42069/api/v1/organizations/ORG/graphs/preprocTrain", preprocTrain,
                "http://10.16.48.121:42069/api/v1/organizations/ORG/graphs/train", train,
                "http://10.16.48.121:42069/api/v1/organizations/ORG/graphs/eval", eval
        );

        Map<String, String> connectorMap = Map.of(
                "http://10.16.48.121:42069/api/v1/connectors/trainedModelConnector1716200887931", "http://10.16.48.121:42069/api/v1/organizations/ORG/graphs/train",
                "http://10.16.48.121:42069/api/v1/connectors/evalTilesConnector1716200887931", "http://10.16.48.121:42069/api/v1/organizations/ORG/graphs/preprocEval",
                "http://10.16.48.121:42069/api/v1/connectors/trainTilesConnector1716200853766", "http://10.16.48.121:42069/api/v1/organizations/ORG/graphs/preprocTrain"
        );

        Query q = new Query(documentMap, connectorMap);
        q.run("http://10.16.48.121:42069/api/v1/organizations/ORG/graphs/eval");
    }
}